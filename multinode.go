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
	return multiExec(ctx, nil, nodes2Exec(mn), query, args...)
}

// Exec runs ExecContext with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included to implement boil.Executor.
func (mn MultiNode) Exec(query string, args ...interface{}) (sql.Result, error) {
	return multiExec(context.Background(), nil, nodes2Exec(mn), query, args...)
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
	return multiQuery(ctx, nil, nodes2Exec(mn), query, args...)
}

// Query runs QueryContext with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included to implement boil.Executor.
func (mn MultiNode) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return multiQuery(context.Background(), nil, nodes2Exec(mn), query, args...)
}

// QueryRowContext runs sql.DB.QueryRowContext on the Nodes in separate Go routines.
// The first error free result is returned immediately.
// If all resulting sql.Row objects contain an error, only the last Row containing an error is returned.
func (mn MultiNode) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return multiQueryRow(ctx, nil, nodes2Exec(mn), query, args...)
}

// QueryRow runs QueryRowContext with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included to implement boil.Executor.
func (mn MultiNode) QueryRow(query string, args ...interface{}) *sql.Row {
	return multiQueryRow(context.Background(), nil, nodes2Exec(mn), query, args...)
}

func (mn MultiNode) txBeginners() []txBeginner {
	txb := make([]txBeginner, len(mn))

	for i, node := range mn {
		txb[i] = node
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
func (mn MultiNode) BeginTx(ctx context.Context, opts *sql.TxOptions) (mtx *MultiTx, err error) {
	mtx = new(MultiTx)
	mtx.tx, err = beginMultiTx(ctx, readOnlyOpts(opts), mn.txBeginners()...)
	return mtx, err
}

// Begin runs BeginTx with context.Background().
// It is highly recommended to stick with the contexted variant in parallel executions.
// This method is primarily included for consistency.
func (mn MultiNode) Begin() (*MultiTx, error) {
	return mn.BeginTx(context.Background(), nil)
}
