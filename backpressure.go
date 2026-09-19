/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4/y"
)

// writeBackpressure implements admission control for writes, bounding how far
// writers may run ahead of the flush/compaction pipeline. It is configured via
// Options.MaxUnflushedMemtables / Options.MaxL0Tables and is completely
// disabled when both are zero (the default), in which case admitWrite is a
// no-op and behavior is identical to not having the gate at all.
//
// Waiting uses a close-and-replace broadcast channel guarded by bp.mu, which
// avoids both busy-polling and the lost-wakeup problem of sync.Cond with
// context cancellation. Correctness against lost wakeups rests on two rules:
//
//  1. Signallers mutate the counts (db.imm, L0 tables, isClosed) FIRST and call
//     bpSignal() afterwards. bpSignal closes the current channel under bp.mu.
//  2. Waiters evaluate the predicate, grab the current channel under bp.mu, and
//     then RE-EVALUATE the predicate before parking on the channel.
//
// If a drain happens between the first check and the channel grab, the second
// check observes it. If it happens after the channel grab, the signaller's
// close (ordered after its mutation) closes the channel the waiter holds.
type writeBackpressure struct {
	maxUnflushedMemtables int
	maxL0Tables           int
	failFast              bool
	enabled               bool

	mu     sync.Mutex
	change chan struct{} // closed & replaced on every drain/close event

	// Cumulative counters; the source of truth for WriteBackpressureStats and
	// mirrored into expvar (see y/metrics.go).
	stalls     atomic.Int64
	rejections atomic.Int64
}

func newWriteBackpressure(opt Options) writeBackpressure {
	return writeBackpressure{
		maxUnflushedMemtables: opt.MaxUnflushedMemtables,
		maxL0Tables:           opt.MaxL0Tables,
		failFast:              opt.BackpressureFailFast,
		enabled:               opt.MaxUnflushedMemtables > 0 || opt.MaxL0Tables > 0,
		change:                make(chan struct{}),
	}
}

// WriteBackpressureStats is a snapshot of the write backpressure state,
// suitable for wiring into monitoring.
type WriteBackpressureStats struct {
	// UnflushedMemtables is the current number of memtables queued for flushing.
	UnflushedMemtables int
	// L0Tables is the current number of tables in L0.
	L0Tables int
	// Throttled reports whether a configured limit is currently exceeded, i.e.
	// new writes would be blocked (or rejected) right now.
	Throttled bool
	// Stalls is the cumulative number of writes that had to wait for the
	// flush/compaction backlog to drain.
	Stalls int64
	// Rejections is the cumulative number of writes rejected with
	// ErrWriteBackpressure (fail-fast mode).
	Rejections int64
}

// WriteBackpressureStats returns the current write backpressure state. When no
// backpressure limits are configured, Throttled is always false and the
// counters are always zero; the counts still reflect the current backlog.
func (db *DB) WriteBackpressureStats() WriteBackpressureStats {
	unflushed, l0 := db.bpCounts()
	return WriteBackpressureStats{
		UnflushedMemtables: unflushed,
		L0Tables:           l0,
		Throttled:          db.bp.overLimit(unflushed, l0),
		Stalls:             db.bp.stalls.Load(),
		Rejections:         db.bp.rejections.Load(),
	}
}

// bpCounts reads the live backlog counts. It must not be called while holding
// db.lock (in any mode) or any level lock.
func (db *DB) bpCounts() (unflushedMemtables, l0Tables int) {
	db.lock.RLock()
	unflushedMemtables = len(db.imm)
	db.lock.RUnlock()
	return unflushedMemtables, db.lc.levels[0].numTables()
}

// overLimit reports whether the given counts exceed the configured limits.
func (bp *writeBackpressure) overLimit(unflushedMemtables, l0Tables int) bool {
	if !bp.enabled {
		return false
	}
	if bp.maxUnflushedMemtables > 0 && unflushedMemtables >= bp.maxUnflushedMemtables {
		return true
	}
	if bp.maxL0Tables > 0 && l0Tables >= bp.maxL0Tables {
		return true
	}
	return false
}

// bpSignal wakes every goroutine waiting in admitWrite. It must be called
// AFTER the underlying counts may have decreased (memtable flushed, L0 tables
// compacted away) or after the DB has been marked closed. Spurious signals are
// harmless: waiters re-check the predicate before proceeding.
func (db *DB) bpSignal() {
	bp := &db.bp
	if !bp.enabled {
		return
	}
	bp.mu.Lock()
	close(bp.change)
	bp.change = make(chan struct{})
	bp.mu.Unlock()
}

// bpWaitChan returns the current broadcast channel.
func (bp *writeBackpressure) waitChan() <-chan struct{} {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.change
}

// bpSetGauges publishes the current state to expvar. It is a no-op unless
// backpressure is enabled and metrics are enabled.
func (db *DB) bpSetGauges(unflushedMemtables, l0Tables int, overLimit bool) {
	if !db.bp.enabled {
		return
	}
	enabled := db.opt.MetricsEnabled
	y.NumUnflushedMemtablesSet(enabled, int64(unflushedMemtables))
	y.NumL0TablesSet(enabled, int64(l0Tables))
	y.WriteBackpressureThrottledSet(enabled, overLimit)
}

// bpRefreshGauges recomputes and publishes the backpressure gauges. It must
// not be called while holding db.lock (in any mode) or any level lock.
func (db *DB) bpRefreshGauges() {
	if !db.bp.enabled {
		return
	}
	unflushed, l0 := db.bpCounts()
	db.bpSetGauges(unflushed, l0, db.bp.overLimit(unflushed, l0))
}

// admitWrite blocks until the write backlog is under the configured limits,
// the context is cancelled, or the DB is closed. With BackpressureFailFast it
// instead returns ErrWriteBackpressure immediately when a limit is reached.
//
// It is called once per write request at admission time (sendToWriteCh) and
// never holds any lock while waiting, so it cannot deadlock the flush or
// compaction goroutines that drain the backlog.
func (db *DB) admitWrite(ctx context.Context) error {
	bp := &db.bp
	if !bp.enabled {
		return nil
	}

	stalled := false
	for {
		unflushed, l0 := db.bpCounts()
		over := bp.overLimit(unflushed, l0)
		db.bpSetGauges(unflushed, l0, over)
		if !over {
			return nil
		}

		if bp.failFast {
			bp.rejections.Add(1)
			y.NumWriteBackpressureRejectionsAdd(db.opt.MetricsEnabled, 1)
			return ErrWriteBackpressure
		}
		if db.IsClosed() {
			return ErrBlockedWrites
		}

		if !stalled {
			stalled = true
			bp.stalls.Add(1)
			y.NumWriteBackpressureStallsAdd(db.opt.MetricsEnabled, 1)
		}

		// Grab the current broadcast channel, then re-check the predicate
		// before parking: a drain that landed between the check above and
		// waitChan() would otherwise be missed (see writeBackpressure docs).
		ch := bp.waitChan()
		unflushed, l0 = db.bpCounts()
		if !bp.overLimit(unflushed, l0) {
			db.bpSetGauges(unflushed, l0, false)
			return nil
		}

		select {
		case <-ch:
			// Backlog may have drained (or the DB is closing); re-evaluate.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
