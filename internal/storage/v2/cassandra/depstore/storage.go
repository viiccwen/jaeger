// Copyright (c) 2019 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
// SPDX-License-Identifier: Apache-2.0

package depstore

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger-idl/model/v1"
	"github.com/jaegertracing/jaeger/internal/metrics"
	"github.com/jaegertracing/jaeger/internal/storage/cassandra"
	casmetrics "github.com/jaegertracing/jaeger/internal/storage/cassandra/metrics"
	storeapi "github.com/jaegertracing/jaeger/internal/storage/v2/api/depstore"
)

// version determines which version of the dependencies table to use.
type version int

const (
	// v1 is used when the dependency table is SASI indexed.
	v1 version = iota

	// v2 is used when the dependency table is NOT SASI indexed.
	v2

	depsInsertStmtV1 = "INSERT INTO dependencies(ts, ts_index, dependencies) VALUES (?, ?, ?)"
	depsInsertStmtV2 = "INSERT INTO dependencies_v2(ts, ts_bucket, dependencies) VALUES (?, ?, ?)"
	depsSelectStmtV1 = "SELECT ts, dependencies FROM dependencies WHERE ts_index >= ? AND ts_index < ?"
	depsSelectStmtV2 = "SELECT ts, dependencies FROM dependencies_v2 WHERE ts_bucket IN ? AND ts >= ? AND ts < ?"

	// TODO: Make this customizable.
	tsBucket = 24 * time.Hour
)

var (
	_ storeapi.Reader = (*DependencyStore)(nil)
	_ storeapi.Writer = (*DependencyStore)(nil)
)

// DependencyStore handles all queries and insertions to Cassandra dependencies.
type DependencyStore struct {
	session                  cassandra.Session
	dependenciesTableMetrics *casmetrics.Table
	logger                   *zap.Logger
	version                  version
}

// NewDependencyStore returns a DependencyStore.
func NewDependencyStore(
	session cassandra.Session,
	metricsFactory metrics.Factory,
	logger *zap.Logger,
) *DependencyStore {
	return &DependencyStore{
		session:                  session,
		dependenciesTableMetrics: casmetrics.NewTable(metricsFactory, "dependencies"),
		logger:                   logger,
		version:                  getDependencyVersion(session),
	}
}

// WriteDependencies writes dependencies to Cassandra.
func (s *DependencyStore) WriteDependencies(
	_ context.Context,
	ts time.Time,
	dependencies []model.DependencyLink,
) error {
	deps := make([]dependency, len(dependencies))
	for i, d := range dependencies {
		deps[i] = dependency{
			Parent: d.Parent,
			Child:  d.Child,
			//nolint:gosec // G115
			CallCount: int64(d.CallCount),
			Source:    string(d.Source),
		}
	}

	var query cassandra.Query
	switch s.version {
	case v1:
		query = s.session.Query(depsInsertStmtV1, ts, ts, deps)
	case v2:
		query = s.session.Query(depsInsertStmtV2, ts, ts.Truncate(tsBucket), deps)
	default:
		return fmt.Errorf("unsupported schema version: %v", s.version)
	}
	return s.dependenciesTableMetrics.Exec(query, s.logger)
}

// GetDependencies returns all interservice dependencies in the requested time range.
func (s *DependencyStore) GetDependencies(
	_ context.Context,
	queryParams storeapi.QueryParameters,
) ([]model.DependencyLink, error) {
	startTime := queryParams.StartTime
	endTime := queryParams.EndTime
	var query cassandra.Query
	switch s.version {
	case v1:
		query = s.session.Query(depsSelectStmtV1, startTime, endTime)
	case v2:
		query = s.session.Query(depsSelectStmtV2, getBuckets(startTime, endTime), startTime, endTime)
	default:
		return nil, fmt.Errorf("unsupported schema version: %v", s.version)
	}
	iter := query.Consistency(cassandra.One).Iter()

	var result []model.DependencyLink
	var dependencies []dependency
	var ts time.Time
	for iter.Scan(&ts, &dependencies) {
		for _, dependency := range dependencies {
			dl := model.DependencyLink{
				Parent: dependency.Parent,
				Child:  dependency.Child,
				//nolint:gosec // G115
				CallCount: uint64(dependency.CallCount),
				Source:    dependency.Source,
			}.ApplyDefaults()
			result = append(result, dl)
		}
	}

	if err := iter.Close(); err != nil {
		s.logger.Error(
			"Failure to read Dependencies",
			zap.Time("endTs", endTime),
			zap.Duration("lookback", endTime.Sub(startTime)),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error reading dependencies from storage: %w", err)
	}
	return result, nil
}

func getBuckets(startTime time.Time, endTime time.Time) []time.Time {
	// TODO: Preallocate the array using some maths and maybe use a pool? This endpoint probably isn't used enough to warrant this.
	var buckets []time.Time
	for ts := startTime.Truncate(tsBucket); ts.Before(endTime); ts = ts.Add(tsBucket) {
		buckets = append(buckets, ts)
	}
	return buckets
}
