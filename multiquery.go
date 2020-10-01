package multidb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
)

// MultiError is a collection of errors which can arise from parallel query execution.
type MultiError struct {
	Errors []error
}

func (me MultiError) Error() string {
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

func (me *MultiError) append(err error) {
	me.Errors = append(me.Errors, err)
}

// check returns a single error if all errors in the MultiError are the same.
// Otherwise, it returns the MultiError containing the multiple errors.
// Returns nil if the are no errors.
func (me MultiError) check() error {
	var first error
	for _, err := range me.Errors {
		switch {
		case err == nil:
			break
		case first == nil:
			first = err
		case err != first:
			return me
		}
	}
	return first
}

type executor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

func nodes2Exec(nodes []*Node) []executor {
	xs := make([]executor, len(nodes))

	for i, node := range nodes {
		xs[i] = node
	}
	return xs
}

func mtx2Exec(mtx []*Tx) []executor {
	xs := make([]executor, len(mtx))

	for i, tx := range mtx {
		xs[i] = tx
	}

	return xs
}

func multiExec(ctx context.Context, xs []executor, query string, args ...interface{}) (sql.Result, chan struct{}, error) {
	rc := make(chan sql.Result, len(xs))
	ec := make(chan error, len(xs))

	var wg sync.WaitGroup
	wg.Add(len(xs))
	for _, x := range xs {
		go func(x executor) {
			res, err := x.ExecContext(ctx, query, args...)
			if err != nil { // Make sure only one of them is returned
				ec <- err
			} else {
				rc <- res
			}
			wg.Done()
		}(x)
	}

	done := make(chan struct{}) // Done signals the caller that all remaining rows are properly closed
	// Close channels when all query routines completed
	go func() {
		wg.Wait()
		close(ec)
		close(rc)
		close(done)
	}()

	if res, ok := <-rc; ok {
		return res, done, nil
	}

	var me MultiError
	for err := range ec {
		me.append(err)
	}
	return nil, nil, me.check()
}

func multiQuery(ctx context.Context, xs []executor, query string, args ...interface{}) (*sql.Rows, chan struct{}, error) {
	rc := make(chan *sql.Rows)
	ec := make(chan error, len(xs))

	var wg sync.WaitGroup
	wg.Add(len(xs))
	for _, x := range xs {
		go func(x executor) {
			rows, err := x.QueryContext(ctx, query, args...)
			if err != nil { // Make sure only one of them is returned
				ec <- err
			} else {
				rc <- rows
			}
			wg.Done()
		}(x)
	}

	// Close channels when all query routines completed
	go func() {
		wg.Wait()
		close(ec)
		close(rc)
	}()

	rows, ok := <-rc
	if ok { // ok will be false if channel closed before any rows
		done := make(chan struct{}) // Done signals the caller that all remaining rows are properly closed
		go func() {
			for rows := range rc { // Drain channel and close unused Rows
				if rows != nil {
					rows.Close()
				}
			}
			close(done)
		}()
		return rows, done, nil
	}

	var me MultiError
	for err := range ec {
		me.append(err)
	}
	return nil, nil, me.check()
}

func multiQueryRow(ctx context.Context, xs []executor, query string, args ...interface{}) (row *sql.Row, done chan struct{}) {
	rc := make(chan *sql.Row)

	var wg sync.WaitGroup
	wg.Add(len(xs))
	for _, x := range xs {
		go func(x executor) {
			rc <- x.QueryRowContext(ctx, query, args...)
			wg.Done()
		}(x)
	}

	// Close channels when all query routines completed
	go func() {
		wg.Wait()
		close(rc)
	}()

	done = make(chan struct{}) // Done signals the caller that all remaining row are properly closed
	defer func() {
		go func() {
			for row := range rc { // Drain channel and close unused Row
				if row != nil {
					row.Scan(&sql.RawBytes{}) // hack to trigger Rows.Close()
				}
			}
			close(done)
		}()
	}()

	for row = range rc {
		if row.Err() == nil {
			return row, done
		}
	}

	return row, done
}
