package multidb

import (
	"context"
	"database/sql"
)

// Tx is a transaction on a node.
// TX implements boil.ContextTransactor and boil.ContextExecutor
type Tx struct {
	*Node
	tx *sql.Tx
}

// Rollback is a wrapper around sql.Tx.Rollback.
func (x *Tx) Rollback() error {
	return x.CheckErr(x.tx.Rollback())
}

// Commit is a wrapper around sql.Tx.Commit.
func (x *Tx) Commit() error {
	return x.CheckErr(x.tx.Commit())
}

// Exec is a wrapper around sql.Tx.ExecContext,
// using context.Background().
func (x *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return x.tx.ExecContext(context.Background(), query, args...)
}

// Query is a wrapper around sql.Tx.QueryContext,
// using context.Background().
func (x *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return x.tx.QueryContext(context.Background(), query, args...)
}

// QueryRow is a wrapper around sql.Tx.QueryRowContext,
// using context.Background().
func (x *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	return x.tx.QueryRowContext(context.Background(), query, args...)
}

// ExecContext is a wrapper around sql.Tx.ExecContext.
func (x *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	res, err := x.tx.ExecContext(ctx, query, args...)
	return res, x.CheckErr(err)
}

// QueryContext is a wrapper around sql.Tx.QueryContext.
func (x *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := x.tx.QueryContext(ctx, query, args...)
	return rows, x.CheckErr(err)
}

// QueryRowContext is a wrapper around sql.Tx.QueryRowContext.
func (x *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	row := x.tx.QueryRowContext(ctx, query, args...)
	x.CheckErr(row.Err())

	return row
}
