// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
	"sync"
)

type txBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func beginMultiTx(ctx context.Context, opts *sql.TxOptions, txb map[string]txBeginner, errCallback ErrCallbackFunc) (map[string]executor, error) {
	type result struct {
		name string
		tx   *sql.Tx
		err  error
	}

	rc := make(chan result, len(txb))

	for name, b := range txb {
		go func(name string, b txBeginner) {
			r := result{name: name}

			r.tx, r.err = b.BeginTx(ctx, opts)
			if r.err != nil {
				r.err = &NodeError{name, r.err}

				if errCallback != nil {
					errCallback(r.err)
				}
			}

			rc <- r
		}(name, b)
	}

	var errs []error

	txs := make(map[string]executor, len(txb))

	for i := 0; i < len(txb); i++ {
		r := <-rc

		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}

		txs[r.name] = r.tx
	}

	if errs != nil {
		if len(txs) == 0 {
			return nil, checkMultiError(errs)
		}

		return txs, checkMultiError(errs)
	}

	return txs, nil
}

// MultiTx holds a slice of open transactions to multiple nodes.
// All methods on this type run their sql.Tx variant in one Go routine per Node.
type MultiTx struct {
	tx          map[string]executor
	wg          sync.WaitGroup
	cancel      context.CancelFunc
	errCallback ErrCallbackFunc
}

// cancelWait cancels a previously running operation on TX
// and waits until all routines are cleaned up.
func (m *MultiTx) cancelWait() {
	if m.cancel != nil {
		m.cancel()
	}

	m.wg.Wait()
}

// Rollback runs sql.Tx.Rollback on the transactions in separate Go routines.
// It waits for all the calls to return.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
//
// Note: this method returns an error even if some rollbacks where executed successfully.
// It is up to the caller to decide what to do with those errors.
// Typically MultiTx calls should only be run against a set of slave databases.
// In such cases Rollback is only used in a defer to tell the hosts that we are done
// and errors can safely be ignored.
//
// Implements boil.Transactor
func (m *MultiTx) Rollback() error {
	m.cancelWait()
	ec := make(chan error, len(m.tx))

	for name, tx := range m.tx {
		go func(name string, tx *sql.Tx) {
			err := tx.Rollback()
			if err != nil {
				err = &NodeError{name, err}
			}

			if m.errCallback != nil {
				m.errCallback(err)
			}

			ec <- err
		}(name, tx.(*sql.Tx))
	}

	var errs []error

	for i := 0; i < len(m.tx); i++ {
		if err := <-ec; err != nil {
			errs = append(errs, err)
		}
	}

	return checkMultiError(errs)
}

// Commit runs sql.Tx.Commit on the transactions in separate Go routines.
// It waits for all the calls to return.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
//
// Note: this method returns an error even if some commits where executed successfully.
// It is up to the caller to decide what to do with those errors.
// Typically MultiTx calls should only be run against a set if slave databases.
// herefore it does not make much sense to Commit.
// If however, you did run this against multiple hosts and some of them failed,
// you'll now have to deal with an inconsistent dataset.
//
// This method is primarily included to implement boil.Transactor
func (m *MultiTx) Commit() error {
	ec := make(chan error, len(m.tx))

	for name, tx := range m.tx {
		go func(name string, tx *sql.Tx) {
			err := tx.Commit()
			if err != nil {
				err = &NodeError{name, err}
			}

			if m.errCallback != nil {
				m.errCallback(err)
			}

			ec <- err
		}(name, tx.(*sql.Tx))
	}

	var errs []error

	for i := 0; i < len(m.tx); i++ {
		if err := <-ec; err != nil {
			errs = append(errs, err)
		}
	}

	return checkMultiError(errs)
}

// Context creates a child context and appends CancelFunc in MultiTx
func (m *MultiTx) context(ctx context.Context) context.Context {
	m.cancelWait()
	ctx, m.cancel = context.WithCancel(ctx)
	return ctx
}

// ExecContext runs sql.Tx.ExecContext on the transactions in separate Go routines.
// The first non-error result is returned immediately
// and errors from the other transactions will be ignored.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
//
// It does not make much sense to run this method against multiple Nodes, as they are ussualy slaves.
// This method is primarily included to implement boil.ContextExecutor.
func (m *MultiTx) ExecContext(ctx context.Context, query string, args ...interface{}) (res sql.Result, err error) {
	return multiExec(m.context(ctx), &m.wg, m.tx, m.errCallback, query, args...)
}

// Exec runs ExecContext with context.Background().
// It is highly recommended to stick with the contexted variant in paralel executions.
// This method is primarily included to implement boil.Executor.
func (m *MultiTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return m.ExecContext(context.Background(), query, args...)
}

// QueryContext runs sql.Tx.QueryContext on the tranactions in separate Go routines.
// The first non-error result is returned immediately
// and errors from the other Nodes will be ignored.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
//
// Implements boil.ContextExecutor.
func (m *MultiTx) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error) {
	return multiQuery(m.context(ctx), &m.wg, m.tx, m.errCallback, query, args...)
}

// Query runs QueryContext with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included to implement boil.Executor.
func (m *MultiTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return m.QueryContext(context.Background(), query, args...)
}

// QueryRowContext runs sql.Tx.QueryRowContext on the tranactions in separate Go routines.
// The first result is returned immediately, regardless if that result has an error.
//
// Errors in sql.Tx.QueryRow are deferred until scan and therefore opaque to this package.
// If you have a choice, stick with a regular QueryContext.
// This method is primarily included to implement boil.Executor.
func (m *MultiTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return multiQueryRow(m.context(ctx), &m.wg, m.tx, m.errCallback, query, args...)
}

// QueryRow wrapper around sql.DB.QueryRow.
// Implements boil.Executor.
// Since errors are deferred until row.Scan, this package cannot monitor such errors.
func (m *MultiTx) QueryRow(query string, args ...interface{}) *sql.Row {
	return m.QueryRowContext(context.Background(), query, args...)
}
