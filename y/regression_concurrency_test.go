/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
)

func TestThrottleLimitsAndReleasesWorkers(t *testing.T) {
	throttle := NewThrottle(2)
	entered := make(chan struct{}, 3)
	started := make(chan struct{}, 3)
	release := make(chan struct{})

	var workers sync.WaitGroup
	workers.Add(3)
	for range [3]struct{}{} {
		go func() {
			defer workers.Done()
			started <- struct{}{}
			require.NoError(t, throttle.Do())
			entered <- struct{}{}
			<-release
			throttle.Done(nil)
		}()
	}

	for range [3]struct{}{} {
		<-started
	}

	<-entered
	<-entered

	require.Zero(t, len(entered))

	select {
	case <-entered:
		t.Fatal("third worker ran before a token was released")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	<-entered
	workers.Wait()
	require.Zero(t, len(entered))
	require.NoError(t, throttle.Finish())
}

func TestThrottlePropagatesWorkerError(t *testing.T) {
	workerErr := errors.New("worker failed")
	throttle := NewThrottle(2)

	require.NoError(t, throttle.Do())
	require.NoError(t, throttle.Do())
	throttle.Done(nil)
	throttle.Done(workerErr)

	require.ErrorIs(t, throttle.Finish(), workerErr)
	require.ErrorIs(t, throttle.Finish(), workerErr)
}

func newTestWaterMark(t *testing.T) *WaterMark {
	t.Helper()

	closer := z.NewCloser(1)
	waterMark := &WaterMark{Name: t.Name()}
	waterMark.Init(closer)
	t.Cleanup(closer.SignalAndWait)

	return waterMark
}

func TestWaterMarkTracksCompletionInOrder(t *testing.T) {
	ctx := context.Background()
	waterMark := newTestWaterMark(t)

	waterMark.Begin(1)
	require.Equal(t, uint64(1), waterMark.LastIndex())
	require.Equal(t, uint64(0), waterMark.DoneUntil())

	waiter := make(chan error, 1)
	go func() {
		waiter <- waterMark.WaitForMark(ctx, 1)
	}()

	waterMark.Done(1)
	require.NoError(t, <-waiter)
	require.Equal(t, uint64(1), waterMark.DoneUntil())

	waterMark.Begin(2)
	waterMark.Done(2)
	require.NoError(t, waterMark.WaitForMark(ctx, 2))
	require.Equal(t, uint64(2), waterMark.DoneUntil())
}

func TestWaterMarkDoesNotAdvanceAcrossGap(t *testing.T) {
	waterMark := newTestWaterMark(t)

	waterMark.Begin(1)
	waterMark.Begin(2)
	waterMark.Begin(3)
	waterMark.Done(2)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, waterMark.WaitForMark(ctx, 2), context.DeadlineExceeded)
	require.Equal(t, uint64(0), waterMark.DoneUntil())

	waterMark.Done(1)
	require.NoError(t, waterMark.WaitForMark(context.Background(), 2))
	require.Equal(t, uint64(2), waterMark.DoneUntil())

	waterMark.Done(3)
	require.NoError(t, waterMark.WaitForMark(context.Background(), 3))
	require.Equal(t, uint64(3), waterMark.DoneUntil())
}

func TestWaterMarkLastIndexIsNotDoneUntil(t *testing.T) {
	waterMark := newTestWaterMark(t)

	waterMark.Begin(5)
	waterMark.Done(5)
	require.NoError(t, waterMark.WaitForMark(context.Background(), 5))
	waterMark.Begin(10)

	require.Equal(t, uint64(10), waterMark.LastIndex())
	require.Equal(t, uint64(5), waterMark.DoneUntil())
}

func TestWaterMarkBeginManyAndDoneMany(t *testing.T) {
	waterMark := newTestWaterMark(t)
	indices := []uint64{4, 5, 6}

	waterMark.BeginMany(indices)
	require.Equal(t, uint64(6), waterMark.LastIndex())

	waterMark.DoneMany(indices)
	require.NoError(t, waterMark.WaitForMark(context.Background(), 6))
	require.Equal(t, uint64(6), waterMark.DoneUntil())
}

func TestWaterMarkSetDoneUntil(t *testing.T) {
	waterMark := newTestWaterMark(t)
	waterMark.SetDoneUntil(42)
	require.Equal(t, uint64(42), waterMark.DoneUntil())
	require.NoError(t, waterMark.WaitForMark(context.Background(), 42))
}

func TestWaterMarkZero(t *testing.T) {
	waterMark := newTestWaterMark(t)
	waterMark.Begin(0)
	waterMark.Done(0)
	require.NoError(t, waterMark.WaitForMark(context.Background(), 0))
	require.Equal(t, uint64(0), waterMark.DoneUntil())
}
