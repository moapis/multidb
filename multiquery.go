// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// MultiError is a collection of errors which can arise from parallel query execution.
type MultiError struct {
	Errors []error
}

func (me *MultiError) Error() string {
	if len(me.Errors) == 0 {
		return "Unknown error"
	}

	var bs strings.Builder
	bs.WriteString("Multiple errors:")
	for i, err := range me.Errors {
		bs.WriteString(fmt.Sprintf(" %d: %v;", i+1, err))
	}
	return bs.String()
}

// checkMultiError returns a single error if all errors in the MultiError are the same.
// Otherwise, it returns the MultiError containing the multiple errors.
// Returns nil if the are no errors.
func checkMultiError(errs []error) error {
	var first error

	for _, err := range errs {

		if ne := new(NodeError); errors.As(err, &ne) {
			err = errors.Unwrap(ne)
		}

		switch {
		case err == nil:
			break
		case first == nil:
			first = err
		case err != first:

			target := &MultiError{
				Errors: make([]error, 0, len(errs)),
			}

			// Purge nil entries
			for _, err := range errs {
				if err != nil {
					target.Errors = append(target.Errors, err)
				}
			}

			return target
		}
	}

	return first
}

// ErrCallbackFunc is called by the individual routines
// executing queries on multiple nodes.
// The function will be called concurently.
type ErrCallbackFunc func(error)

// NodeError is passed to ErrCallbackFunc functions in order to
// destinguish between DB nodes in concurrent query executions.
type NodeError struct {
	name    string
	wrapped error
}

func (ne *NodeError) Error() string {
	return fmt.Sprintf("Node %s: %v", ne.name, ne.wrapped)
}

// Name of the node that experienced the error
func (ne *NodeError) Name() string {
	return ne.name
}

func (ne *NodeError) Unwrap() error {
	return ne.wrapped
}

type executor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

func multiExec(ctx context.Context, wg *sync.WaitGroup, xs map[string]executor, errCallback ErrCallbackFunc, query string, args ...interface{}) (sql.Result, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		res sql.Result
		err error
	}

	rc := make(chan result, len(xs))

	if wg != nil {
		wg.Add(len(xs))
	}

	for name, x := range xs {
		go func(name string, x executor) {
			var r result

			r.res, r.err = x.ExecContext(ctx, query, args...)
			if r.err != nil {
				r.err = &NodeError{name, r.err}

				if errCallback != nil {
					errCallback(r.err)
				}
			}

			rc <- r

			if wg != nil {
				wg.Done()
			}

		}(name, x)
	}

	var errs []error

	for i := len(xs); i > 0; i-- {
		r := <-rc

		if r.err == nil {
			return r.res, nil
		}

		errs = append(errs, r.err)
	}

	return nil, checkMultiError(errs)
}

func multiQuery(ctx context.Context, wg *sync.WaitGroup, xs map[string]executor, errCallback ErrCallbackFunc, query string, args ...interface{}) (*sql.Rows, error) {
	type result struct {
		rows *sql.Rows
		err  error
	}

	// Buffered channel works ~40% faster, regardless of draining.
	rc := make(chan result, len(xs))

	if wg != nil {
		wg.Add(len(xs))
	}

	for name, x := range xs {
		go func(name string, x executor) {
			var r result

			r.rows, r.err = x.QueryContext(ctx, query, args...)
			if r.err != nil {
				r.err = &NodeError{name, r.err}

				if errCallback != nil {
					errCallback(r.err)
				}
			}

			rc <- r

			if wg != nil {
				wg.Done()
			}
		}(name, x)
	}

	var errs []error

	for i := len(xs); i > 0; i-- {
		r := <-rc

		if r.err == nil {

			// Drain channel and close unused Rows
			go func(i int) {
				for ; i > 0; i-- {
					if r := <-rc; r.rows != nil {
						r.rows.Close()
					}
				}
			}(i)

			return r.rows, nil
		}

		errs = append(errs, r.err)
	}

	return nil, checkMultiError(errs)
}

func multiQueryRow(ctx context.Context, wg *sync.WaitGroup, xs map[string]executor, errCallback ErrCallbackFunc, query string, args ...interface{}) (row *sql.Row) {
	rc := make(chan *sql.Row, len(xs))

	if wg != nil {
		wg.Add(len(xs))
	}

	for name, x := range xs {
		go func(name string, x executor) {
			row := x.QueryRowContext(ctx, query, args...)

			if row != nil { // Prevent panic in benchmarks
				if err := row.Err(); err != nil && errCallback != nil {
					errCallback(&NodeError{name, err})
				}
			}

			rc <- row

			if wg != nil {
				wg.Done()
			}
		}(name, x)
	}

	for i := len(xs); i > 0; i-- {
		row = <-rc

		if row != nil && row.Err() == nil {

			// Drain channel and close unused Rows
			go func() {
				for ; i > 0; i-- {
					if row := <-rc; row != nil {
						row.Scan(&sql.RawBytes{}) // hack to trigger Rows.Close()
					}
				}
			}()

			break
		}

	}

	return row
}
