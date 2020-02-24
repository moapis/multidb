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

func nodes2Exec(nodes []*Node) (xs []executor) {
	for _, n := range nodes {
		xs = append(xs, n)
	}
	return xs
}

func mtx2Exec(mtx []*Tx) (xs []executor) {
	for _, tx := range mtx {
		xs = append(xs, tx)
	}
	return xs
}

func multiExec(ctx context.Context, xs []executor, query string, args ...interface{}) (sql.Result, error) {
	rc := make(chan sql.Result, len(xs))
	ec := make(chan error, len(xs))
	for _, x := range xs {
		go func(x executor) {
			res, err := x.ExecContext(ctx, query, args...)
			switch { // Make sure only one of them is returned
			case err != nil:
				ec <- err
			case res != nil:
				rc <- res
			}
		}(x)
	}

	var me MultiError
	for i := 0; i < len(xs); i++ {
		select {
		case err := <-ec:
			me.append(err)
		case res := <-rc: // Return on the first success
			return res, nil
		}
	}
	return nil, me.check()
}

func multiQuery(ctx context.Context, xs []executor, query string, args ...interface{}) (*sql.Rows, chan struct{}, error) {
	rc := make(chan *sql.Rows)
	done := make(chan struct{}) // Done signals the caller that all routines finished
	ec := make(chan error, len(xs))

	var wg sync.WaitGroup
	wg.Add(len(xs))
	for _, x := range xs {
		go func(x executor) {
			rows, err := x.QueryContext(ctx, query, args...)
			switch { // Make sure only one of them is returned
			case err != nil:
				ec <- err
			case rows != nil:
				rc <- rows
			}
			wg.Done()
		}(x)
	}

	// Signal all routines done
	go func() {
		wg.Wait()
		close(ec)
		close(done)
		close(rc)
	}()

	rows, ok := <-rc
	if ok { // ok will be false if channel closed before any rows
		go func() {
			for rows := range rc { // Drain channel and close unused Rows
				rows.Close()
			}
		}()
		return rows, done, nil
	}

	var me MultiError
	for err := range ec {
		me.append(err)

	}
	return nil, done, me.check()
}

func multiQueryRow(ctx context.Context, xs []executor, query string, args ...interface{}) *sql.Row {
	rc := make(chan *sql.Row, len(xs))
	for _, x := range xs {
		go func(x executor) {
			rc <- x.QueryRowContext(ctx, query, args...)
		}(x)
	}
	return <-rc
}
