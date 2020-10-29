// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
)

// MultiNode holds multiple DB Nodes.
// All methods on this type run their sql.DB variant in one Go routine per Node.
type MultiNode struct {
	nodes       map[string]executor
	errCallback ErrCallbackFunc
}

// ExecContext runs sql.DB.ExecContext on the Nodes in separate Go routines.
// The first non-error result is returned immediately
// and errors from the other Nodes will be ignored.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
//
// It does not make much sense to run this method against multiple Nodes, as they are usually slaves.
// This method is primarily included to implement boil.ContextExecutor.
func (m *MultiNode) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return multiExec(ctx, nil, m.nodes, m.errCallback, query, args...)
}

// Exec runs ExecContext with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included to implement boil.Executor.
func (m *MultiNode) Exec(query string, args ...interface{}) (sql.Result, error) {
	return multiExec(context.Background(), nil, m.nodes, m.errCallback, query, args...)
}

// QueryContext runs sql.DB.QueryContext on the Nodes in separate Go routines.
// The first non-error result is returned immediately
// and errors from the other Nodes will be ignored.
//
// It is important to cancel the context as soon as possible after scanning the rows.
// Preferably, before any next operation on MultiNode.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
//
// Implements boil.ContextExecutor.
func (m *MultiNode) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return multiQuery(ctx, nil, m.nodes, m.errCallback, query, args...)
}

// Query runs QueryContext with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included to implement boil.Executor.
func (m *MultiNode) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return multiQuery(context.Background(), nil, m.nodes, m.errCallback, query, args...)
}

// QueryRowContext runs sql.DB.QueryRowContext on the Nodes in separate Go routines.
// The first error free result is returned immediately.
// If all resulting sql.Row objects contain an error, only the last Row containing an error is returned.
//
// It is important to cancel the context as soon as possible after scanning the row.
// Preferably, before any next operation on MultiNode.
func (m *MultiNode) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return multiQueryRow(ctx, nil, m.nodes, m.errCallback, query, args...)
}

// QueryRow runs QueryRowContext with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included to implement boil.Executor.
func (m *MultiNode) QueryRow(query string, args ...interface{}) *sql.Row {
	return multiQueryRow(context.Background(), nil, m.nodes, m.errCallback, query, args...)
}

func (m *MultiNode) txBeginners() map[string]txBeginner {
	txb := make(map[string]txBeginner, len(m.nodes))

	for name, node := range m.nodes {
		txb[name] = node.(txBeginner)
	}

	return txb
}

// BeginTx runs sql.DB.BeginTx on the Nodes in separate Go routines.
// The transactions are created in ReadOnly mode.
// It waits for all the calls to return or the context to expire.
// If you have enough nodes available, you might want to set short
// timeout values on the context to fail fast on non-responding database hosts.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
//
// Note: this method can return both a valid Tx and an error value,
// in case any (but not all) node calls fails.
// Tx will carry fewer amount of entries than requested.
// This breaks the common `if err != nil` convention,
// but we want to leave the descission whetter to proceed or not, up to the caller.
func (m *MultiNode) BeginTx(ctx context.Context, opts *sql.TxOptions) (mtx *MultiTx, err error) {
	mtx = &MultiTx{
		errCallback: m.errCallback,
	}

	mtx.tx, err = beginMultiTx(ctx, readOnlyOpts(opts), m.txBeginners(), m.errCallback)
	return mtx, err
}

// Begin runs BeginTx with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included for consistency.
func (m *MultiNode) Begin() (*MultiTx, error) {
	return m.BeginTx(context.Background(), nil)
}
