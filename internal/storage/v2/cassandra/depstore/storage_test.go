// Copyright (c) 2019 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
// Copyright (c) 2019 Uber Technologies, Inc.
// SPDX-License-Identifier: Apache-2.0

package depstore

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger-idl/model/v1"
	"github.com/jaegertracing/jaeger/internal/metrics"
	"github.com/jaegertracing/jaeger/internal/storage/cassandra"
	casmetrics "github.com/jaegertracing/jaeger/internal/storage/cassandra/metrics"
	"github.com/jaegertracing/jaeger/internal/storage/cassandra/mocks"
	storeapi "github.com/jaegertracing/jaeger/internal/storage/v2/api/depstore"
	"github.com/jaegertracing/jaeger/internal/testutils"
)

func TestGetDependencyVersion(t *testing.T) {
	tests := []struct {
		name     string
		queryErr error
		expected version
	}{
		{name: "v1", queryErr: errors.New("table does not exist"), expected: v1},
		{name: "v2", expected: v2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &mocks.Session{}
			query := &mocks.Query{}
			session.On("Query", "SELECT ts from dependencies_v2 limit 1;", mock.Anything).Return(query)
			query.On("Exec").Return(test.queryErr)
			assert.Equal(t, test.expected, getDependencyVersion(session))
		})
	}
}

func TestNewDependencyStore(t *testing.T) {
	session := &mocks.Session{}
	query := &mocks.Query{}
	session.On("Query", "SELECT ts from dependencies_v2 limit 1;", mock.Anything).Return(query)
	query.On("Exec").Return(nil)

	store := NewDependencyStore(session, metrics.NullFactory, zap.NewNop())
	assert.Equal(t, v2, store.version)
}

func newTestDependencyStore(session cassandra.Session, logger *zap.Logger, schemaVersion version) *DependencyStore {
	return &DependencyStore{
		session:                  session,
		dependenciesTableMetrics: casmetrics.NewTable(metrics.NullFactory, "dependencies"),
		logger:                   logger,
		version:                  schemaVersion,
	}
}

func TestDependencyStoreWrite(t *testing.T) {
	tests := []struct {
		name              string
		version           version
		expectedStatement string
	}{
		{
			name:              "v1",
			version:           v1,
			expectedStatement: depsInsertStmtV1,
		},
		{
			name:              "v2",
			version:           v2,
			expectedStatement: depsInsertStmtV2,
		},
	}
	ts := time.Date(2017, time.January, 24, 11, 15, 17, 12345, time.UTC)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &mocks.Session{}
			query := &mocks.Query{}
			query.On("Exec").Return(nil)
			var args []any
			session.On("Query", test.expectedStatement, mock.MatchedBy(func(actual []any) bool {
				args = actual
				return true
			})).Return(query)
			store := newTestDependencyStore(session, zap.NewNop(), test.version)

			err := store.WriteDependencies(t.Context(), ts, []model.DependencyLink{{
				Parent:    "a",
				Child:     "b",
				CallCount: 42,
				Source:    model.JaegerDependencyLinkSource,
			}})
			require.NoError(t, err)
			require.Len(t, args, 3)
			assert.Equal(t, ts, args[0])
			if test.version == v1 {
				assert.Equal(t, ts, args[1])
			} else {
				assert.Equal(t, ts.Truncate(tsBucket), args[1])
			}
			assert.Equal(t, []dependency{{Parent: "a", Child: "b", CallCount: 42, Source: "jaeger"}}, args[2])
		})
	}
}

func TestDependencyStoreGetDependencies(t *testing.T) {
	tests := []struct {
		name              string
		version           version
		expectedStatement string
		queryErr          error
	}{
		{name: "success v1", version: v1, expectedStatement: depsSelectStmtV1},
		{name: "success v2", version: v2, expectedStatement: depsSelectStmtV2},
		{name: "failure v1", version: v1, expectedStatement: depsSelectStmtV1, queryErr: errors.New("query error")},
		{name: "failure v2", version: v2, expectedStatement: depsSelectStmtV2, queryErr: errors.New("query error")},
	}
	startTime := time.Date(2017, time.January, 24, 11, 15, 17, 12345, time.UTC)
	endTime := startTime.Add(48 * time.Hour)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, logBuffer := testutils.NewLogger()
			session := &mocks.Session{}
			iter := &mocks.Iterator{}
			rows := [][]dependency{
				{{Parent: "a", Child: "b", CallCount: 1}},
				{{Parent: "b", Child: "c", CallCount: 2, Source: "custom"}},
			}
			iter.On("Scan", mock.MatchedBy(func(args []any) bool {
				if len(rows) == 0 {
					return false
				}
				for _, arg := range args {
					if dependencies, ok := arg.(*[]dependency); ok {
						*dependencies = rows[0]
					}
				}
				rows = rows[1:]
				return true
			})).Return(true)
			iter.On("Scan", mock.Anything).Return(false)
			iter.On("Close").Return(test.queryErr)
			query := &mocks.Query{}
			query.On("Consistency", cassandra.One).Return(query)
			query.On("Iter").Return(iter)
			var args []any
			session.On("Query", test.expectedStatement, mock.MatchedBy(func(actual []any) bool {
				args = actual
				return true
			})).Return(query)
			store := newTestDependencyStore(session, logger, test.version)

			dependencies, err := store.GetDependencies(t.Context(), storeapi.QueryParameters{
				StartTime: startTime,
				EndTime:   endTime,
			})
			if test.queryErr != nil {
				require.ErrorContains(t, err, "error reading dependencies from storage: query error")
				assert.Contains(t, logBuffer.String(), "Failure to read Dependencies")
				return
			}
			require.NoError(t, err)
			assert.Empty(t, logBuffer.String())
			assert.Equal(t, []model.DependencyLink{
				{Parent: "a", Child: "b", CallCount: 1, Source: model.JaegerDependencyLinkSource},
				{Parent: "b", Child: "c", CallCount: 2, Source: "custom"},
			}, dependencies)
			if test.version == v1 {
				assert.Equal(t, []any{startTime, endTime}, args)
			} else {
				assert.Equal(t, []any{getBuckets(startTime, endTime), startTime, endTime}, args)
			}
		})
	}
}

func TestGetBuckets(t *testing.T) {
	startTime := time.Date(2017, time.January, 24, 11, 15, 17, 12345, time.UTC)
	endTime := time.Date(2017, time.January, 26, 11, 15, 17, 12345, time.UTC)
	assert.Equal(t, []time.Time{
		time.Date(2017, time.January, 24, 0, 0, 0, 0, time.UTC),
		time.Date(2017, time.January, 25, 0, 0, 0, 0, time.UTC),
		time.Date(2017, time.January, 26, 0, 0, 0, 0, time.UTC),
	}, getBuckets(startTime, endTime))
}

func TestDependencyStoreUnsupportedVersion(t *testing.T) {
	store := &DependencyStore{
		session: &mocks.Session{},
		logger:  zap.NewNop(),
		version: version(999),
	}
	err := store.WriteDependencies(t.Context(), time.Now(), []model.DependencyLink{{Parent: "parent", Child: "child"}})
	require.ErrorContains(t, err, "unsupported schema version")
	_, err = store.GetDependencies(t.Context(), storeapi.QueryParameters{
		StartTime: time.Now(),
		EndTime:   time.Now(),
	})
	require.ErrorContains(t, err, "unsupported schema version")
}
