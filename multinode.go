package multidb

import (
	"context"
	"database/sql"
)

// MultiNode holds a slice of Nodes.
// All methods on this type run their sql.DB variant in one Go routine per Node.
type MultiNode []*Node

// ExecContext runs sql.DB.ExecContext on the Nodes in separate Go routines.
// The first non-error result is returned immediately
// and errors from the other Nodes will be ignored.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
//
// It does not make much sense to run this method against multiple Nodes, as they are usually slaves.
// This method is primarily included to implement boil.ContextExecutor.
func (mn MultiNode) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	res, _, err := multiExec(ctx, nodes2Exec(mn), query, args...)
	return res, err
}

// Exec runs ExecContext with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included to implement boil.Executor.
func (mn MultiNode) Exec(query string, args ...interface{}) (sql.Result, error) {
	res, _, err := multiExec(context.Background(), nodes2Exec(mn), query, args...)
	return res, err
}

// QueryContext runs sql.DB.QueryContext on the Nodes in separate Go routines.
// The first non-error result is returned immediately
// and errors from the other Nodes will be ignored.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
//
// Implements boil.ContextExecutor.
func (mn MultiNode) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, _, err := multiQuery(ctx, nodes2Exec(mn), query, args...)
	return rows, err
}

// Query runs QueryContext with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included to implement boil.Executor.
func (mn MultiNode) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows, _, err := multiQuery(context.Background(), nodes2Exec(mn), query, args...)
	return rows, err
}

// QueryRowContext runs sql.DB.QueryRowContext on the Nodes in separate Go routines.
// The first result is returned immediately, regardless if that result has an error.
//
// Errors in sql.DB.QueryRow are deferred until scan and therefore opaque to this package.
// If you have a choice, stick with a regular QueryContext.
// This method is primarily included to implement boil.Executor.
func (mn MultiNode) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	row, _ := multiQueryRow(ctx, nodes2Exec(mn), query, args...)
	return row
}

// QueryRow runs QueryRowContext with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included to implement boil.Executor.
func (mn MultiNode) QueryRow(query string, args ...interface{}) *sql.Row {
	row, _ := multiQueryRow(context.Background(), nodes2Exec(mn), query, args...)
	return row
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
func (mn MultiNode) BeginTx(ctx context.Context, opts *sql.TxOptions) (*MultiTx, error) {
	tc := make(chan *Tx, len(mn))
	ec := make(chan error, len(mn))
	for _, n := range mn {
		go func(n *Node) {
			tx, err := n.BeginTx(ctx, readOnlyOpts(opts))
			switch { // Make sure only one of them is returned
			case err != nil:
				ec <- err
			case tx != nil:
				tc <- tx
			}
		}(n)
	}

	var me MultiError
	m := &MultiTx{tx: make([]*Tx, 0, len(mn))}
	for i := 0; i < len(mn); i++ {
		select {
		case err := <-ec:
			me.append(err)
		case tx := <-tc:
			m.tx = append(m.tx, tx)
		}
	}
	if len(m.tx) == 0 {
		return nil, me.check()
	}
	return m, me.check()
}

// Begin runs BeginTx with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included for consistency.
func (mn MultiNode) Begin() (*MultiTx, error) {
	return mn.BeginTx(context.Background(), nil)
}
