package multidb

import (
	"context"
	"database/sql"
)

// Tx is a transaction on a node
type Tx struct {
	*Node
	tx *sql.Tx
}

// Rollback is a wrapper around sql.Tx.Rollback.
// Implements boil.Transactor and boil.ContextTransactor
func (x *Tx) Rollback() error {
	return x.CheckErr(x.tx.Rollback())
}

// Commit is a wrapper around sql.Tx.Commit.
// Implements boil.Transactor and boil.ContextTransactor
func (x *Tx) Commit() error {
	return x.CheckErr(x.tx.Commit())
}

// Exec is a wrapper around sql.Tx.Exec.
// Implements boil.Executor
func (x *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	res, err := x.tx.Exec(query, args...)
	return res, x.CheckErr(err)
}

// Query is a wrapper around sql.Tx.Query.
// Implements boil.Executor
func (x *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := x.tx.Query(query, args...)
	return rows, x.CheckErr(err)
}

// QueryRow is a wrapper around sql.Tx.QueryRow.
// Implements boil.Executor
// Since errors are defered untill row.Scan, this package cannot monitor such errors.
func (x *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	return x.tx.QueryRow(query, args...)
}

// ExecContext is a wrapper around sql.Tx.Exec.
// Implements boil.ContextExecutor
func (x *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	res, err := x.tx.ExecContext(ctx, query, args...)
	return res, x.CheckErr(err)
}

// QueryContext is a wrapper around sql.Tx.Query.
// Implements boil.ContextExecutor
func (x *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := x.tx.QueryContext(ctx, query, args...)
	return rows, x.CheckErr(err)
}

// QueryRowContext is a wrapper around sql.Tx.QueryRow.
// Implements boil.ContextExecutor
// Since errors are defered untill row.Scan, this package cannot monitor such errors.
func (x *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return x.tx.QueryRowContext(ctx, query, args...)
}
