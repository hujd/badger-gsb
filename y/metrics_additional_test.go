/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"fmt"
	"sync/atomic"
	"testing"

	"expvar"
	"github.com/stretchr/testify/require"
)

var metricsTestRun uint64

func intMetricValue(metric *expvar.Int) int64 {
	return metric.Value()
}

func TestMetricsRespectEnabledFlag(t *testing.T) {
	runID := atomic.AddUint64(&metricsTestRun, 1)

	intMetrics := []struct {
		name   string
		add    func(bool, int64)
		metric *expvar.Int
	}{
		{"iterators", NumIteratorsCreatedAdd, numIteratorsCreated},
		{"gets-with-results", NumGetsWithResultsAdd, numGetsWithResults},
		{"reads-vlog", NumReadsVlogAdd, numReadsVlog},
		{"bytes-written-user", NumBytesWrittenUserAdd, numBytesWrittenUser},
		{"writes-vlog", NumWritesVlogAdd, numWritesVlog},
		{"bytes-read-vlog", NumBytesReadsVlogAdd, numBytesReadVlog},
		{"bytes-read-lsm", NumBytesReadsLSMAdd, numBytesReadLSM},
		{"bytes-written-vlog", NumBytesWrittenVlogAdd, numBytesVlogWritten},
		{"bytes-written-l0", NumBytesWrittenToL0Add, numBytesWrittenToL0},
		{"gets", NumGetsAdd, numGets},
		{"puts", NumPutsAdd, numPuts},
		{"memtable-gets", NumMemtableGetsAdd, numMemtableGets},
		{"compaction-tables", NumCompactionTablesAdd, numCompactionTables},
	}

	for _, metric := range intMetrics {
		t.Run(metric.name+"-disabled", func(t *testing.T) {
			before := intMetricValue(metric.metric)
			metric.add(false, 100)
			require.Equal(t, before, intMetricValue(metric.metric))

			metric.add(true, 7)
			require.Equal(t, before+7, intMetricValue(metric.metric))
		})
	}

	mapMetrics := []struct {
		name   string
		add    func(bool, string, int64)
		metric *expvar.Map
	}{
		{"compaction-written", NumBytesCompactionWrittenAdd, numBytesCompactionWritten},
		{"bloom-hits", NumLSMBloomHitsAdd, numLSMBloomHits},
		{"lsm-gets", NumLSMGetsAdd, numLSMGets},
	}

	for _, metric := range mapMetrics {
		t.Run(metric.name, func(t *testing.T) {
			disabledKey := metric.name + "-disabled"
			enabledKey := fmt.Sprintf("%s-enabled-%d", metric.name, runID)

			metric.add(false, disabledKey, 100)
			require.Nil(t, metric.metric.Get(disabledKey))

			metric.add(true, enabledKey, 9)
			metric.add(true, enabledKey, 3)
			require.Equal(t, "12", metric.metric.Get(enabledKey).String())
		})
	}

	mapSetters := []struct {
		name   string
		set    func(bool, string, expvar.Var)
		get    func(bool, string) expvar.Var
		metric *expvar.Map
	}{
		{"lsm-size", LSMSizeSet, LSMSizeGet, lsmSize},
		{"vlog-size", VlogSizeSet, VlogSizeGet, vlogSize},
		{"pending-writes", PendingWritesSet, nil, pendingWrites},
	}

	for _, metric := range mapSetters {
		t.Run(metric.name, func(t *testing.T) {
			disabled := new(expvar.String)
			disabled.Set("disabled")
			metric.set(false, metric.name+"-disabled", disabled)
			require.Nil(t, metric.metric.Get(metric.name+"-disabled"))

			enabled := new(expvar.String)
			enabled.Set("enabled")
			metric.set(true, metric.name+"-enabled", enabled)
			require.Equal(t, `"enabled"`, metric.metric.Get(metric.name+"-enabled").String())

			if metric.get != nil {
				require.Nil(t, metric.get(false, metric.name+"-enabled"))
				require.Equal(t, `"enabled"`, metric.get(true, metric.name+"-enabled").String())
			}
		})
	}
}
