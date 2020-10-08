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

func mtx2Exec(mtx []*sql.Tx) []executor {
	xs := make([]executor, len(mtx))

	for i, tx := range mtx {
		xs[i] = tx
	}

	return xs
}

func multiExec(ctx context.Context, wg *sync.WaitGroup, xs []executor, query string, args ...interface{}) (sql.Result, error) {
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

	for _, x := range xs {
		go func(x executor) {
			var r result
			r.res, r.err = x.ExecContext(ctx, query, args...)
			rc <- r

			if wg != nil {
				wg.Done()
			}

		}(x)
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

func multiQuery(ctx context.Context, wg *sync.WaitGroup, xs []executor, query string, args ...interface{}) (*sql.Rows, error) {
	type result struct {
		rows *sql.Rows
		err  error
	}

	// Buffered channel works ~40% faster, regardless of draining.
	rc := make(chan result, len(xs))

	if wg != nil {
		wg.Add(len(xs))
	}

	for i := 0; i < len(xs); i++ {
		go func(x executor) {
			var r result
			r.rows, r.err = x.QueryContext(ctx, query, args...)
			rc <- r

			if wg != nil {
				wg.Done()
			}
		}(xs[i])
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

func multiQueryRow(ctx context.Context, wg *sync.WaitGroup, xs []executor, query string, args ...interface{}) (row *sql.Row) {
	rc := make(chan *sql.Row, len(xs))

	if wg != nil {
		wg.Add(len(xs))
	}

	for _, x := range xs {
		go func(x executor) {
			rc <- x.QueryRowContext(ctx, query, args...)

			if wg != nil {
				wg.Done()
			}
		}(x)
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
