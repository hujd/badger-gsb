/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4/y"
)

// ErrWriteBackpressure is returned by write APIs when write backpressure is
// configured in reject mode (WriteBackpressureReject) and one of the configured
// limits (MaxPendingMemtables or MaxL0Tables) has been reached. It is a
// transient condition: the write was not applied and no data was rejected for
// being invalid, so callers should retry it later once flush/compaction has
// caught up. Use errors.Is(err, ErrWriteBackpressure) to detect it.
var ErrWriteBackpressure = errors.New(
	"Write rejected due to backpressure: too many pending memtables or L0 tables; retry later")

func (db *DB) incBlocked() {
	db.bpBlocked.Add(1)
	y.BPBlockedAdd(db.opt.MetricsEnabled, 1)
}

func (db *DB) incRejected() {
	db.bpRejected.Add(1)
	y.BPRejectedAdd(db.opt.MetricsEnabled, 1)
}

// numL0Tables returns the current number of Level 0 tables.
func (db *DB) numL0Tables() int {
	return db.lc.levels[0].numTables()
}

// numPendingMemtablesLocked returns the number of in-memory memtables not yet
// flushed to Level 0: the immutable memtables queued to (or being processed by)
// the flush goroutine plus the active memtable being written. Caller must hold
// db.lock (read or write).
func (db *DB) numPendingMemtablesLocked() int {
	n := len(db.imm)
	if db.mt != nil {
		n++
	}
	return n
}

// recalcPendingGaugeLocked resets the lock-free pending-memtable gauge from the
// actual imm/mt state. Caller must hold db.lock (read or write).
func (db *DB) recalcPendingGaugeLocked() {
	db.bpPendingGauge.Store(int32(db.numPendingMemtablesLocked()))
}

// backpressureActiveLocked reports whether either of the user-configured
// backpressure limits has been reached. A zero limit (the default) disables the
// corresponding check, so with both limits unset this always returns false and
// the write path behaves exactly as it did before backpressure support.
// Caller must hold db.lock (read or write) for the memtable count; the L0 count
// is read under its own level lock and is slightly racy by design (see
// numTables).
func (db *DB) backpressureActiveLocked() bool {
	if lim := db.opt.MaxPendingMemtables; lim > 0 && db.numPendingMemtablesLocked() >= lim {
		return true
	}
	if lim := db.opt.MaxL0Tables; lim > 0 && db.numL0Tables() >= lim {
		return true
	}
	return false
}

// signalWriteProgress wakes writers blocked in admitWrite and/or
// ensureRoomForWrite because backpressure limits were reached. It must be called
// whenever one of the pressure inputs may have decreased (memtable flushed, L0
// tables compacted away) or when the DB is closing. Every blocked writer
// re-checks the predicate under db.lock before waiting again, so spurious
// broadcasts are harmless.
func (db *DB) signalWriteProgress() {
	// Gate (edge-triggered) wakeup for context-aware admission waiters.
	if db.writeGate != nil {
		select {
		case db.writeGate <- struct{}{}:
		default:
		}
	}
	// flushCond wakes the (serial) writeRequests goroutine blocked in
	// ensureRoomForWrite. Broadcast is correct for that cond; the relay below
	// happens on the gate channel, not on flushCond.
	if db.flushCond != nil {
		db.flushCond.Broadcast()
	}
}

// admitWrite is called in the caller's goroutine, before a write is enqueued on
// writeCh. With no limits configured it is a no-op. In block mode it waits
// (interruptibly via ctx) until the number of unflushed memtables and L0
// tables drops below the configured limits; in reject mode it fails fast with
// ErrWriteBackpressure.
//
// Waiting here is serialized with pressure changes through db.lock: every
// signal arrives between two predicate evaluations, so wakeups cannot be lost.
// Woken waiters relay one gate token before re-checking, so a single signal
// advances the whole waiting queue.
func (db *DB) admitWrite(ctx context.Context) error {
	if db.opt.MaxPendingMemtables <= 0 && db.opt.MaxL0Tables <= 0 {
		return nil
	}

	var blocked bool
	for {
		db.lock.RLock()
		closed := db.IsClosed() || db.blockWrites.Load() == 1
		active := !closed && db.backpressureActiveLocked()
		db.lock.RUnlock()

		if closed {
			if blocked {
				db.incBlocked()
			}
			return ErrBlockedWrites
		}
		if !active {
			if blocked {
				db.incBlocked()
			}
			return nil
		}

		if db.opt.BackpressureMode == WriteBackpressureReject {
			db.incRejected()
			db.publishBackpressureGauges()
			return ErrWriteBackpressure
		}

		blocked = true
		db.publishBackpressureGauges()
		if ctx == nil {
			ctx = context.Background()
		}
		select {
		case <-ctx.Done():
			db.incBlocked()
			return ctx.Err()
		case <-db.writeGateClosed:
			db.incBlocked()
			return ErrBlockedWrites
		case <-db.writeGate:
			// Relay the token so other waiters can make progress even if the
			// predicate is still active for this one, then re-check.
			db.relayWriteGate()
		}
	}
}

// relayWriteGate forwards one writeGate token, if no token is already pending.
// Called by a woken waiter that is going to keep waiting, guaranteeing an
// edge-triggered signal is never consumed by a waiter that stays blocked.
func (db *DB) relayWriteGate() {
	select {
	case db.writeGate <- struct{}{}:
	default:
	}
}

// WriteBackpressureStats exposes write-backpressure state for monitoring.
type WriteBackpressureStats struct {
	// UnflushedMemtables is the current number of in-memory memtables not yet
	// flushed to L0 (active memtable + immutable memtables queued for flush).
	UnflushedMemtables int
	// NumL0Tables is the current number of Level 0 tables.
	NumL0Tables int
	// Backpressured reports whether a write would currently be blocked/rejected,
	// i.e. whether either configured limit has been reached.
	Backpressured bool
	// BlockedTotal is the cumulative number of writes that were held back in
	// block mode and either resumed or gave up (context cancellation / close).
	BlockedTotal uint64
	// RejectedTotal is the cumulative number of writes failed with
	// ErrWriteBackpressure in reject mode.
	RejectedTotal uint64
}

// WriteBackpressureStats returns a snapshot of the write-backpressure state.
// All fields are populated regardless of whether limits are configured, so the
// counters can be wired into monitoring before the limits are enabled.
func (db *DB) WriteBackpressureStats() WriteBackpressureStats {
	db.lock.RLock()
	nPending := db.numPendingMemtablesLocked()
	active := db.backpressureActiveLocked()
	db.lock.RUnlock()

	return WriteBackpressureStats{
		UnflushedMemtables: nPending,
		NumL0Tables:        db.numL0Tables(),
		Backpressured:      active,
		BlockedTotal:       db.bpBlocked.Load(),
		RejectedTotal:      db.bpRejected.Load(),
	}
}

// publishBackpressureGauges refreshes the point-in-time expvar gauges exposed
// for monitoring. Counters are maintained live by admitWrite.
func (db *DB) publishBackpressureGauges() {
	if !db.opt.MetricsEnabled {
		return
	}
	pending := db.bpPendingGauge.Load()
	l0 := int64(db.numL0Tables())
	active := int64(0)
	// active is derived from atomically-maintained gauges only, so this is safe
	// to call from contexts already holding db.lock (compaction under
	// DropPrefix). Config reads are immutable after Open.
	if lim := db.opt.MaxPendingMemtables; lim > 0 && int(pending) >= lim {
		active = 1
	}
	if lim := db.opt.MaxL0Tables; lim > 0 && int(l0) >= lim {
		active = 1
	}
	key := db.opt.Dir
	y.BPPendingMemtablesSet(true, key, int64(pending))
	y.BPL0TablesSet(true, key, l0)
	y.BPActiveSet(true, key, active)
}
