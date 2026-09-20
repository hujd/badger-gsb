package y

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
)

func newTestWaterMark(t *testing.T) *WaterMark {
	t.Helper()

	closer := z.NewCloser(1)
	watermark := &WaterMark{Name: t.Name()}
	watermark.Init(closer)

	t.Cleanup(closer.SignalAndWait)
	return watermark
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not reached")
}

func TestWaterMarkLastIndexIsIndependentOfDoneUntil(t *testing.T) {
	watermark := newTestWaterMark(t)

	watermark.Begin(1)
	watermark.Begin(2)
	watermark.Begin(3)

	require.Eventually(t, func() bool { return watermark.LastIndex() == 3 }, time.Second, time.Millisecond)
	require.EqualValues(t, 0, watermark.DoneUntil(), "nothing has finished processing")

	watermark.Done(1)
	waitFor(t, func() bool { return watermark.DoneUntil() == 1 })

	require.EqualValues(t, 3, watermark.LastIndex(), "LastIndex must report the latest Begin, not processing progress")
	require.EqualValues(t, 1, watermark.DoneUntil())

	watermark.Done(2)
	watermark.Done(3)
	waitFor(t, func() bool { return watermark.DoneUntil() == 3 })
	require.EqualValues(t, 3, watermark.LastIndex())
}

func TestWaterMarkDoneUntilAdvancesOnlyInOrder(t *testing.T) {
	watermark := newTestWaterMark(t)

	watermark.BeginMany([]uint64{1, 2, 3})
	require.Eventually(t, func() bool { return watermark.LastIndex() == 3 }, time.Second, time.Millisecond)

	watermark.DoneMany([]uint64{1, 3})
	waitFor(t, func() bool { return watermark.DoneUntil() == 1 })

	watermark.Done(2)
	waitFor(t, func() bool { return watermark.DoneUntil() == 3 })
}

func TestWaterMarkWaitForMark(t *testing.T) {
	watermark := newTestWaterMark(t)

	watermark.Begin(10)

	errCh := make(chan error, 1)
	go func() { errCh <- watermark.WaitForMark(context.Background(), 10) }()

	consistentlyWaiting := true
	for i := 0; i < 20; i++ {
		select {
		case err := <-errCh:
			if consistentlyWaiting {
				t.Fatalf("WaitForMark returned before mark: %v", err)
			}
		default:
		}
		time.Sleep(time.Millisecond)
	}

	watermark.Done(10)
	require.NoError(t, <-errCh)

	watermark.SetDoneUntil(12)
	require.NoError(t, watermark.WaitForMark(context.Background(), 11))
}

func TestWaterMarkWaitForMarkContextCancel(t *testing.T) {
	watermark := newTestWaterMark(t)
	watermark.Begin(7)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, watermark.WaitForMark(ctx, 7), context.DeadlineExceeded)

	watermark.Done(7)
	waitFor(t, func() bool { return watermark.DoneUntil() == 7 })
}
