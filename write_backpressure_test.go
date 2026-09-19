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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// pinL0 appends n empty tables to L0 so the flush goroutine stalls in
// addLevel0Table (same pattern as l0_backpressure_test.go), giving the test
// full control over when the flush/compaction backlog drains.
func pinL0(t *testing.T, db *DB, n int) {
	t.Helper()
	l0 := db.lc.levels[0]
	l0.Lock()
	for i := 0; i < n; i++ {
		tab := createEmptyTable(db)
		l0.tables = append(l0.tables, tab)
		l0.addSize(tab)
	}
	l0.Unlock()
}

// bpTestOptions returns options with compaction disabled and small memtables,
// so tests control the L0 table count and fill memtables quickly.
func bpTestOptions() Options {
	opt := getTestOptions("")
	opt.InMemory = true
	opt.NumCompactors = 0
	opt.NumMemtables = 2
	opt.NumLevelZeroTables = 2
	opt.NumLevelZeroTablesStall = 10
	opt.MemTableSize = 1 << 15
	opt.ValueThreshold = 1 << 10
	return opt
}

// fillUnflushed writes until n memtables are queued for flushing (db.imm),
// which requires the flush goroutine to be stalled (see pinL0).
func fillUnflushed(t *testing.T, db *DB, n int) {
	t.Helper()
	val := make([]byte, 512)
	for i := 0; db.WriteBackpressureStats().UnflushedMemtables < n; i++ {
		require.Less(t, i, 1000, "could not fill the unflushed memtable queue")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := db.UpdateContext(ctx, func(txn *Txn) error {
			return txn.Set([]byte(fmt.Sprintf("fill-%08d", i)), val)
		})
		cancel()
		require.NoError(t, err)
	}
}

// TestWriteBackpressureFailFastMemtables verifies that with
// BackpressureFailFast=true, writes are rejected with ErrWriteBackpressure
// once the unflushed-memtable limit is reached, and recover after a drain.
func TestWriteBackpressureFailFastMemtables(t *testing.T) {
	opt := bpTestOptions()
	opt.MaxUnflushedMemtables = 1
	opt.BackpressureFailFast = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		pinL0(t, db, opt.NumLevelZeroTablesStall)
		defer drainL0(t, db, opt.NumLevelZeroTablesStall)

		rejectMetric := expvar.Get("badger_write_backpressure_reject_num").(*expvar.Int)
		rejectsBefore := rejectMetric.Value()

		// Writes succeed until one memtable is queued for flushing; after that
		// every write must be rejected with ErrWriteBackpressure.
		var bpErr error
		for i := 0; i < 1000; i++ {
			err := db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key-%08d", i)), make([]byte, 512))
			})
			if err != nil {
				bpErr = err
				break
			}
		}
		require.Error(t, bpErr, "expected writes to be rejected once the limit was reached")
		require.True(t, errors.Is(bpErr, ErrWriteBackpressure),
			"expected ErrWriteBackpressure, got: %v", bpErr)

		stats := db.WriteBackpressureStats()
		require.GreaterOrEqual(t, stats.Rejections, int64(1))
		require.GreaterOrEqual(t, stats.UnflushedMemtables, 1)
		require.True(t, stats.Throttled)
		require.Equal(t, int64(0), stats.Stalls, "fail-fast mode must not record stalls")
		require.GreaterOrEqual(t, rejectMetric.Value()-rejectsBefore, int64(1),
			"expvar rejection counter should increase")

		// Draining L0 lets the flush goroutine catch up; writes succeed again.
		drainL0(t, db, opt.NumLevelZeroTablesStall)
		require.Eventually(t, func() bool {
			return db.WriteBackpressureStats().UnflushedMemtables == 0
		}, 5*time.Second, time.Millisecond)
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte("after-drain"), []byte("ok"))
		}))
		require.False(t, db.WriteBackpressureStats().Throttled)
	})
}

// TestWriteBackpressureWaitCtxCancel verifies that in wait mode a throttled
// write blocks until the caller's context is cancelled, and that the
// cancellation surfaces the context's error (not a data error).
func TestWriteBackpressureWaitCtxCancel(t *testing.T) {
	opt := bpTestOptions()
	opt.MaxUnflushedMemtables = 1
	// BackpressureFailFast defaults to false: wait mode.

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		pinL0(t, db, opt.NumLevelZeroTablesStall)
		defer drainL0(t, db, opt.NumLevelZeroTablesStall)

		fillUnflushed(t, db, 1)

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := db.UpdateContext(ctx, func(txn *Txn) error {
			return txn.Set([]byte("blocked-key"), []byte("v"))
		})
		require.True(t, errors.Is(err, context.DeadlineExceeded),
			"expected context.DeadlineExceeded, got: %v", err)
		require.GreaterOrEqual(t, time.Since(start), 250*time.Millisecond,
			"the write should have waited for the backlog instead of failing fast")

		stats := db.WriteBackpressureStats()
		require.GreaterOrEqual(t, stats.Stalls, int64(1))
		require.Equal(t, int64(0), stats.Rejections)
		require.True(t, stats.Throttled)

		// The cancelled write must not have been applied.
		require.ErrorIs(t, db.View(func(txn *Txn) error {
			_, err := txn.Get([]byte("blocked-key"))
			return err
		}), ErrKeyNotFound)
	})
}

// TestWriteBackpressureWaitUnblocksAfterDrain verifies that a write parked in
// wait mode is woken promptly once the backlog drains, and then completes.
func TestWriteBackpressureWaitUnblocksAfterDrain(t *testing.T) {
	opt := bpTestOptions()
	opt.MaxUnflushedMemtables = 1

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		pinL0(t, db, opt.NumLevelZeroTablesStall)
		fillUnflushed(t, db, 1)

		errCh := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			errCh <- db.UpdateContext(ctx, func(txn *Txn) error {
				return txn.Set([]byte("parked-key"), []byte("v"))
			})
		}()

		// Wait until the writer is actually parked in admitWrite.
		require.Eventually(t, func() bool {
			return db.WriteBackpressureStats().Stalls >= 1
		}, 5*time.Second, time.Millisecond)

		// The write must still be parked (no spurious completions).
		select {
		case err := <-errCh:
			t.Fatalf("write completed while the backlog was still pinned: %v", err)
		case <-time.After(100 * time.Millisecond):
		}

		// Drain L0; the flush goroutine catches up, imm shrinks, and the parked
		// writer must be woken by the drain signal.
		drainL0(t, db, opt.NumLevelZeroTablesStall)
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("parked write was not woken after the backlog drained")
		}

		require.NoError(t, db.View(func(txn *Txn) error {
			_, err := txn.Get([]byte("parked-key"))
			return err
		}))
	})
}

// TestWriteBackpressureL0Limit verifies the L0 table limit independently of
// the memtable limit.
func TestWriteBackpressureL0Limit(t *testing.T) {
	opt := bpTestOptions()
	opt.MaxL0Tables = 3
	opt.BackpressureFailFast = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		pinL0(t, db, 3)
		defer drainL0(t, db, 3)

		err := db.Update(func(txn *Txn) error {
			return txn.Set([]byte("k"), []byte("v"))
		})
		require.True(t, errors.Is(err, ErrWriteBackpressure),
			"expected ErrWriteBackpressure with L0 at the limit, got: %v", err)

		stats := db.WriteBackpressureStats()
		require.Equal(t, 3, stats.L0Tables)
		require.True(t, stats.Throttled)
		require.GreaterOrEqual(t, stats.Rejections, int64(1))

		// Drop below the limit; writes go through again.
		drainL0(t, db, 1)
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte("k"), []byte("v"))
		}))
	})
}

// TestWriteBackpressureCloseUnblocksWaiter verifies that closing the DB wakes
// writers parked in admitWrite instead of hanging them (or Close) forever.
func TestWriteBackpressureCloseUnblocksWaiter(t *testing.T) {
	opt := bpTestOptions()
	opt.MaxUnflushedMemtables = 1

	db, err := Open(opt)
	require.NoError(t, err)

	pinL0(t, db, opt.NumLevelZeroTablesStall)
	fillUnflushed(t, db, 1)

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		errCh <- db.UpdateContext(ctx, func(txn *Txn) error {
			return txn.Set([]byte("parked-key"), []byte("v"))
		})
	}()

	require.Eventually(t, func() bool {
		return db.WriteBackpressureStats().Stalls >= 1
	}, 5*time.Second, time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()

	select {
	case err := <-errCh:
		require.True(t, errors.Is(err, ErrBlockedWrites),
			"parked writer should be woken with ErrBlockedWrites on close, got: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("parked writer was not woken by Close")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("db.Close() hung with a writer parked in admitWrite")
	}
}

// TestWriteBackpressureDisabledByDefault verifies that with no limits
// configured, writes are never throttled and the counters stay at zero.
func TestWriteBackpressureDisabledByDefault(t *testing.T) {
	opt := getTestOptions("")
	opt.InMemory = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		for i := 0; i < 100; i++ {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key-%08d", i)), []byte("v"))
			}))
		}
		stats := db.WriteBackpressureStats()
		require.False(t, stats.Throttled)
		require.Equal(t, int64(0), stats.Stalls)
		require.Equal(t, int64(0), stats.Rejections)
	})
}

// TestWriteBackpressureNegativeLimitRejected verifies option validation.
func TestWriteBackpressureNegativeLimitRejected(t *testing.T) {
	opt := getTestOptions("")
	opt.InMemory = true
	opt.MaxUnflushedMemtables = -1
	_, err := Open(opt)
	require.Error(t, err)

	opt = getTestOptions("")
	opt.InMemory = true
	opt.MaxL0Tables = -1
	_, err = Open(opt)
	require.Error(t, err)
}

// TestWriteBackpressureOnDiskSmoke exercises the gate on a disk-backed DB,
// including a restart (which replays the WAL into db.imm at Open).
func TestWriteBackpressureOnDiskSmoke(t *testing.T) {
	dir := t.TempDir()
	opt := getTestOptions(dir)
	opt.MemTableSize = 1 << 15
	opt.ValueThreshold = 1 << 10
	opt.MaxUnflushedMemtables = 3
	opt.MaxL0Tables = 10

	db, err := Open(opt)
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte(fmt.Sprintf("key-%08d", i)), make([]byte, 512))
		}))
	}
	require.NoError(t, db.Close())

	// Reopen with the same limits; the gate must work across the WAL replay.
	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	stats := db.WriteBackpressureStats()
	require.False(t, stats.Throttled)
	for i := 0; i < 100; i++ {
		require.NoError(t, db.UpdateContext(context.Background(), func(txn *Txn) error {
			return txn.Set([]byte(fmt.Sprintf("key2-%08d", i)), make([]byte, 512))
		}))
	}
	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("key-00000000"))
		return err
	}))
}

// TestWriteBackpressureConcurrentStress hammers the DB with concurrent writers
// while flush and compaction run for real, with both limits engaged. It
// asserts there is no deadlock, no lost wakeup, and no data loss. Run under
// -race.
func TestWriteBackpressureConcurrentStress(t *testing.T) {
	opt := getTestOptions("")
	opt.InMemory = true
	opt.MemTableSize = 1 << 14
	opt.NumMemtables = 3
	opt.NumCompactors = 2
	opt.NumLevelZeroTables = 2
	opt.NumLevelZeroTablesStall = 8
	opt.MaxUnflushedMemtables = 1
	opt.MaxL0Tables = 6
	opt.ValueThreshold = 1 << 10

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		const writers = 8
		const writesPerWriter = 200
		val := make([]byte, 512)

		var wg sync.WaitGroup
		errCh := make(chan error, writers)
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for i := 0; i < writesPerWriter; i++ {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					err := db.UpdateContext(ctx, func(txn *Txn) error {
						key := fmt.Sprintf("w%02d-key-%05d", id, i)
						return txn.Set([]byte(key), val)
					})
					cancel()
					if err != nil {
						errCh <- fmt.Errorf("writer %d write %d: %w", id, i, err)
						return
					}
				}
			}(w)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			require.NoError(t, err)
		}

		// All writes must be readable.
		require.NoError(t, db.View(func(txn *Txn) error {
			for w := 0; w < writers; w++ {
				for i := 0; i < writesPerWriter; i++ {
					key := fmt.Sprintf("w%02d-key-%05d", w, i)
					if _, err := txn.Get([]byte(key)); err != nil {
						return fmt.Errorf("get %s: %w", key, err)
					}
				}
			}
			return nil
		}))

		stats := db.WriteBackpressureStats()
		require.Equal(t, int64(0), stats.Rejections)
		t.Logf("stats after stress: %+v", stats)
	})
}
