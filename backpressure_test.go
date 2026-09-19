/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/table"
)

// bpTestOptions returns small, in-memory options tuned to make the
// backpressure limits engage quickly and deterministically.
func bpTestOptions() Options {
	opt := getTestOptions("")
	opt.InMemory = true
	opt.NumCompactors = 0 // L0 count is fully controlled by pinning/draining.
	opt.MemTableSize = 1 << 15
	opt.ValueThreshold = 1 << 10
	return opt
}

// pinL0 appends n fake empty tables to L0. The references it adds are dropped
// by drainAllL0, which tests must call before the DB is closed.
func pinL0(db *DB, n int) {
	l0 := db.lc.levels[0]
	l0.Lock()
	for i := 0; i < n; i++ {
		tab := createEmptyTable(db)
		l0.tables = append(l0.tables, tab)
		l0.addSize(tab)
	}
	l0.Unlock()
}

// drainAllL0 removes every table from L0 and signals the stall cond plus the
// write gate, like an L0 compaction followed by signalL0Drained.
func drainAllL0(t *testing.T, db *DB) {
	t.Helper()
	l0 := db.lc.levels[0]
	l0.Lock()
	toDrop := append([]*table.Table{}, l0.tables...)
	l0.tables = l0.tables[:0]
	for _, tab := range toDrop {
		l0.subtractSize(tab)
	}
	l0.Unlock()
	require.NoError(t, decrRefs(toDrop))
	db.signalWriteProgress()
	l0.signalL0Drained()
}

// writeUntilPendingMemtables writes until the number of unflushed memtables
// reaches want (active + immutable). Requires flushChan capacity >= want-1 and
// the flush goroutine parked (L0 pinned high).
func writeUntilPendingMemtables(t *testing.T, db *DB, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	i := 0
	for db.WriteBackpressureStats().UnflushedMemtables < want {
		require.Falsef(t, time.Now().After(deadline),
			"pending memtables %d never reached %d",
			db.WriteBackpressureStats().UnflushedMemtables, want)
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte(fmt.Sprintf("pad-%08d", i)), make([]byte, 512))
		}))
		i++
	}
}

func expvarIntValue(t *testing.T, name string) int64 {
	t.Helper()
	v := expvar.Get(name)
	require.NotNil(t, v, "missing expvar %s", name)
	n, err := strconv.ParseInt(v.String(), 10, 64)
	require.NoError(t, err)
	return n
}

func expvarMapValue(t *testing.T, name, key string) int64 {
	t.Helper()
	m := expvar.Get(name)
	require.NotNil(t, m, "missing expvar map %s", name)
	mp, ok := m.(*expvar.Map)
	require.True(t, ok)
	v := mp.Get(key)
	require.NotNil(t, v, "missing expvar %s[%s]", name, key)
	n, err := strconv.ParseInt(v.String(), 10, 64)
	require.NoError(t, err)
	return n
}

// TestBPDisabledByDefault verifies that without configured limits the write
// path is unaffected: counters are zero and a write with a tight context
// succeeds immediately (the context is never consulted).
func TestBPDisabledByDefault(t *testing.T) {
	opt := bpTestOptions()

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		pinL0(db, opt.NumLevelZeroTablesStall)

		s := db.WriteBackpressureStats()
		require.False(t, s.Backpressured, "no limits: never backpressured")
		require.Equal(t, 1, s.UnflushedMemtables) // just the active memtable
		require.Equal(t, opt.NumLevelZeroTablesStall, s.NumL0Tables)
		require.Zero(t, s.BlockedTotal)
		require.Zero(t, s.RejectedTotal)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, db.Update(func(txn *Txn) error {
			txn.WithContext(ctx)
			return txn.Set([]byte("k"), []byte("v"))
		}))
	})
}

// TestBPRejectMemtables: reject mode + MaxPendingMemtables fails fast with
// ErrWriteBackpressure once the cap is reached and accepts again after flush.
func TestBPRejectMemtables(t *testing.T) {
	opt := bpTestOptions()
	opt.NumMemtables = 6
	opt.NumLevelZeroTablesStall = 50 // keep L0 away from the internal stall.
	opt.MaxPendingMemtables = 3
	opt.BackpressureMode = WriteBackpressureReject

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Park the flush goroutine in addLevel0Table so flushed memtables stop
	// leaving the pipeline and the pending count climbs to the cap.
	pinL0(db, opt.NumLevelZeroTablesStall)
	writeUntilPendingMemtables(t, db, opt.MaxPendingMemtables)

	s := db.WriteBackpressureStats()
	require.True(t, s.Backpressured)
	require.Equal(t, opt.MaxPendingMemtables, s.UnflushedMemtables)

	err = db.Update(func(txn *Txn) error {
		return txn.Set([]byte("k-rejected"), []byte("v"))
	})
	require.ErrorIs(t, err, ErrWriteBackpressure)
	require.GreaterOrEqual(t, db.WriteBackpressureStats().RejectedTotal, uint64(1))
	require.Zero(t, db.WriteBackpressureStats().BlockedTotal)

	drainAllL0(t, db)
	require.Eventually(t, func() bool {
		return !db.WriteBackpressureStats().Backpressured
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("k-ok"), []byte("v"))
	}))
}

// TestBPRejectL0 rejects writes as soon as the L0 cap is reached and accepts
// again once L0 drains below it.
func TestBPRejectL0(t *testing.T) {
	opt := bpTestOptions()
	opt.MaxL0Tables = 3
	opt.NumLevelZeroTables = 2
	opt.NumLevelZeroTablesStall = 10
	opt.BackpressureMode = WriteBackpressureReject

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		pinL0(db, opt.MaxL0Tables)

		s := db.WriteBackpressureStats()
		require.True(t, s.Backpressured)
		require.Equal(t, opt.MaxL0Tables, s.NumL0Tables)

		err := db.Update(func(txn *Txn) error {
			return txn.Set([]byte("k"), []byte("v"))
		})
		require.ErrorIs(t, err, ErrWriteBackpressure)

		drainAllL0(t, db)
		require.Eventually(t, func() bool {
			return !db.WriteBackpressureStats().Backpressured
		}, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte("k"), []byte("v"))
		}))
	})
}

// TestBPBlockCancellable verifies block mode waits while pressure is on and
// that cancelling the context unblocks the commit with the context error,
// counted as exactly one blocked write.
func TestBPBlockCancellable(t *testing.T) {
	opt := bpTestOptions()
	opt.NumMemtables = 6
	opt.NumLevelZeroTablesStall = 50
	opt.MaxPendingMemtables = 3
	opt.BackpressureMode = WriteBackpressureBlock

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	pinL0(db, opt.NumLevelZeroTablesStall)
	writeUntilPendingMemtables(t, db, opt.MaxPendingMemtables)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- db.Update(func(txn *Txn) error {
			txn.WithContext(ctx)
			return txn.Set([]byte("k-blocked"), []byte("v"))
		})
	}()

	// It must wait while pressure is on.
	select {
	case <-done:
		t.Fatal("blocked write returned before resume/cancel")
	case <-time.After(300 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled blocked write did not return")
	}
	require.Equal(t, uint64(1), db.WriteBackpressureStats().BlockedTotal)

	drainAllL0(t, db)
}

// TestBPBlockResumes verifies a blocked write resumes promptly (event-driven,
// no polling quantum) once pressure clears and completes successfully.
func TestBPBlockResumes(t *testing.T) {
	opt := bpTestOptions()
	opt.NumMemtables = 6
	opt.NumLevelZeroTablesStall = 50
	opt.MaxPendingMemtables = 3
	opt.BackpressureMode = WriteBackpressureBlock

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	pinL0(db, opt.NumLevelZeroTablesStall)
	writeUntilPendingMemtables(t, db, opt.MaxPendingMemtables)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- db.Update(func(txn *Txn) error {
			txn.WithContext(ctx)
			return txn.Set([]byte("k-resume"), make([]byte, 512))
		})
	}()

	select {
	case <-done:
		t.Fatal("write should be blocked while pressure is on")
	case <-time.After(300 * time.Millisecond):
	}

	start := time.Now()
	drainAllL0(t, db)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("blocked write did not resume after pressure cleared")
	}
	require.Less(t, time.Since(start), 2*time.Second,
		"resume should be event-driven, not polled")
	require.Equal(t, uint64(1), db.WriteBackpressureStats().BlockedTotal)

	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("k-resume"))
		return err
	}))
}

// TestBPConcurrentNoDeadlock runs many concurrent writers through block mode
// while L0 oscillates across the cap, then closes under load. Run with -race.
func TestBPConcurrentNoDeadlock(t *testing.T) {
	opt := bpTestOptions()
	opt.NumMemtables = 3
	opt.NumLevelZeroTablesStall = 40
	opt.MaxL0Tables = 3
	opt.BackpressureMode = WriteBackpressureBlock

	db, err := Open(opt)
	require.NoError(t, err)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Drives L0 across the cap so writers cycle block -> resume.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			pinL0(db, opt.MaxL0Tables)
			time.Sleep(2 * time.Millisecond)
			drainAllL0(t, db)
		}
	}()

	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for i := 0; i < 3000; i++ {
				select {
				case <-stop:
					return
				default:
				}
				err := db.Update(func(txn *Txn) error {
					txn.WithContext(ctx)
					return txn.Set([]byte(fmt.Sprintf("w%02d-%08d", id, i)),
						make([]byte, 256))
				})
				if errors.Is(err, ErrBlockedWrites) ||
					errors.Is(err, errNoRoom) ||
					errors.Is(err, context.DeadlineExceeded) {
					return
				}
				require.NoError(t, err)
			}
		}(w)
	}

	time.Sleep(2 * time.Second)
	close(stop)
	require.NoError(t, db.Close())
	wg.Wait()
}

// TestBPOptionsValidation covers invalid option combinations.
func TestBPOptionsValidation(t *testing.T) {
	_, err := Open(bpTestOptions().WithMaxPendingMemtables(-1))
	require.Error(t, err)

	_, err = Open(bpTestOptions().WithMaxL0Tables(-1))
	require.Error(t, err)

	// MaxL0Tables above the internal stall threshold is rejected (stall=15).
	_, err = Open(bpTestOptions().WithMaxL0Tables(20))
	require.Error(t, err)

	db, err := Open(bpTestOptions().
		WithMaxPendingMemtables(2).
		WithMaxL0Tables(4).
		WithBackpressureMode(WriteBackpressureReject))
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

// TestBPExpvarMetrics asserts gauges and counters are exposed via expvar.
func TestBPExpvarMetrics(t *testing.T) {
	opt := bpTestOptions()
	opt.MaxL0Tables = 2
	opt.NumLevelZeroTablesStall = 10
	opt.BackpressureMode = WriteBackpressureReject
	opt.MetricsEnabled = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		pinL0(db, opt.MaxL0Tables)
		err := db.Update(func(txn *Txn) error {
			return txn.Set([]byte("k"), []byte("v"))
		})
		require.ErrorIs(t, err, ErrWriteBackpressure)

		db.publishBackpressureGauges()
		require.Equal(t, int64(opt.MaxL0Tables),
			expvarMapValue(t, "badger_backpressure_current_num_l0", opt.Dir))
		require.Equal(t, int64(1),
			expvarMapValue(t, "badger_backpressure_active", opt.Dir))
		require.GreaterOrEqual(t,
			expvarIntValue(t, "badger_backpressure_rejected_total"), int64(1))

		drainAllL0(t, db)
		db.publishBackpressureGauges()
		require.Equal(t, int64(0),
			expvarMapValue(t, "badger_backpressure_active", opt.Dir))
	})
}

// TestBPDropPrefixWhileBlocked guards against a deadlock between DropPrefix's
// blockWrite (which waits for the write goroutine to exit) and user writes
// parked in admitWrite under block mode. blockWrite must wake such writers;
// after DropPrefix completes, writes must work again.
func TestBPDropPrefixWhileBlocked(t *testing.T) {
	opt := bpTestOptions()
	opt.NumMemtables = 6
	opt.NumLevelZeroTablesStall = 50
	opt.MaxPendingMemtables = 2
	opt.BackpressureMode = WriteBackpressureBlock

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create backpressure by parking the flusher.
	pinL0(db, opt.NumLevelZeroTablesStall)

	// Keep a writer blocked in admitWrite while DropPrefix runs.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = db.Update(func(txn *Txn) error {
				return txn.Set([]byte("bp-key"), []byte("v"))
			})
		}
	}()

	time.Sleep(200 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- db.DropPrefix([]byte("pre:")) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("DropPrefix hung while writers were blocked on backpressure")
	}

	close(stop)
	wg.Wait()

	// Writes work again after the drop.
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("after"), []byte("v"))
	}))

	drainAllL0(t, db)
}

// TestBPWriteBatchReject exercises the main ingestion path: in reject mode a
// WriteBatch write surfaces ErrWriteBackpressure and the batch can be retried
// after pressure clears.
func TestBPWriteBatchReject(t *testing.T) {
	opt := bpTestOptions()
	opt.MaxL0Tables = 3
	opt.NumLevelZeroTables = 2
	opt.NumLevelZeroTablesStall = 10
	opt.BackpressureMode = WriteBackpressureReject

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		pinL0(db, opt.MaxL0Tables)

		wb := db.NewWriteBatch()
		defer wb.Cancel()
		// Batch commits are async; the rejection is recorded and surfaced by
		// Flush (and by subsequent Set/Delete via wb.Error).
		require.NoError(t, wb.Set([]byte("bk"), []byte("bv")))
		require.ErrorIs(t, wb.Flush(), ErrWriteBackpressure)

		drainAllL0(t, db)
		require.Eventually(t, func() bool {
			return !db.WriteBackpressureStats().Backpressured
		}, 5*time.Second, 10*time.Millisecond)

		wb2 := db.NewWriteBatch()
		defer wb2.Cancel()
		require.NoError(t, wb2.Set([]byte("bk"), []byte("bv")))
		require.NoError(t, wb2.Flush())
	})
}

// TestBPWriteBatchContextCancel verifies a WriteBatch commit blocked in block
// mode returns the context error once ctx is cancelled.
func TestBPWriteBatchContextCancel(t *testing.T) {
	opt := bpTestOptions()
	opt.NumMemtables = 6
	opt.NumLevelZeroTablesStall = 50
	opt.MaxPendingMemtables = 3
	opt.BackpressureMode = WriteBackpressureBlock

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	pinL0(db, opt.NumLevelZeroTablesStall)
	writeUntilPendingMemtables(t, db, opt.MaxPendingMemtables)

	ctx, cancel := context.WithCancel(context.Background())
	wb := db.NewWriteBatch().WithContext(ctx)

	done := make(chan error, 1)
	go func() {
		// Enough keys to force an internal commit synchronously.
		for i := 0; i < 100000; i++ {
			if err := wb.Set(
				[]byte(fmt.Sprintf("bk-%08d", i)), make([]byte, 256)); err != nil {
				done <- err
				return
			}
		}
		done <- wb.Flush()
	}()

	select {
	case <-done:
		t.Fatal("batch commit should block under pressure")
	case <-time.After(500 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled batch did not return")
	}
	wb.Cancel()
	drainAllL0(t, db)
}
