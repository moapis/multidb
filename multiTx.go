package multidb

import (
	"context"
	"database/sql"
)

// MultiTx holds a slice of open transactions to multiple nodes.
// All methods on this type run their sql.Tx variant in one Go routine per Node.
type MultiTx []*Tx

func (mtx *MultiTx) append(tx *Tx) {
	*mtx = append(*mtx, tx)
}

// Rollback runs sql.Tx.Rollback on the transactions in seperate Go routines.
// It waits for all the calls to return.
//
// The following errors can be returned:
// - If all nodes respond with the same error, that exact error is returned as-is.
// - If there is a variaty of errors, they will be embedded in a MultiError return.
//
// Note: this method returns an error even if some rollbacks where executed succesfully.
// It is up to the caller to decide what to do with those errors.
// Typically MultiTx calls should only be run against a set if slave databases.
// In such cases Rollback is only used in a defer to tell the hosts that we are done
// and errors can safely be ignored.
//
// Implements boil.Transactor
func (mtx MultiTx) Rollback() error {
	ec := make(chan error, len(mtx))
	for _, tx := range mtx {
		go func(tx *Tx) {
			err := tx.Rollback()
			ec <- err
		}(tx)
	}
	var me MultiError
	for i := 0; i < len(mtx); i++ {
		if err := <-ec; err != nil {
			me.append(err)
		}
	}
	return me.check()
}

// Commit runs sql.Tx.Commit on the transactions in seperate Go routines.
// It waits for all the calls to return.
//
// The following errors can be returned:
// - If all nodes respond with the same error, that exact error is returned as-is.
// - If there is a variaty of errors, they will be embedded in a MultiError return.
//
// Note: this method returns an error even if some commits where executed succesfully.
// It is up to the caller to decide what to do with those errors.
// Typically MultiTx calls should only be run against a set if slave databases.
// herefore it does not make much sense to Commit.
// If however, you did run this against multiple hosts and some of them failed,
// you'll now have to deal with an inconsistent dataset.
//
// This method is primarily included to implement boil.Transactor
func (mtx MultiTx) Commit() error {
	ec := make(chan error, len(mtx))
	for _, tx := range mtx {
		go func(tx *Tx) {
			ec <- tx.Commit()
		}(tx)
	}
	var me MultiError
	for i := 0; i < len(mtx); i++ {
		if err := <-ec; err != nil {
			me.append(err)
		}
	}
	return me.check()
}

// ExecContext runs sql.Tx.ExecContext on the transactions in seperate Go routines.
// The first non-error result is returned immediatly
// and errors from the other transactions will be ignored.
//
// The following errors can be returned:
// - If all nodes respond with the same error, that exact error is returned as-is.
// - If the is a variaty of errors, they will be embedded in a MultiError return.
//
// It does not make much sense to run this method against multiple Nodes, as they are ussualy slaves.
// This method is primarily included to implement boil.ContextExecutor.
func (mtx MultiTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return multiExec(ctx, mtx2Exec(mtx), query, args...)
}

// Exec runs ExecContext with context.Background().
// It is highly recommended to stick with the contexed variant in paralell executions.
// This method is primarily included to implement boil.Executor.
func (mtx MultiTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return multiExec(context.Background(), mtx2Exec(mtx), query, args...)
}

// QueryContext runs sql.Tx.QueryContext on the tranactions in seperate Go routines.
// The first non-error result is returned immediatly
// and errors from the other Nodes will be ignored.
//
// The following errors can be returned:
// - If all nodes respond with the same error, that exact error is returned as-is.
// - If the is a variaty of errors, they will be embedded in a MultiError return.
//
// Implements boil.ContextExecutor.
func (mtx MultiTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return multiQuery(ctx, mtx2Exec(mtx), query, args...)
}

// Query runs QueryContext with context.Background().
// It is highly recommended to stick with the contexed variant in paralell executions.
// This method is primarily included to implement boil.Executor.
func (mtx MultiTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return multiQuery(context.Background(), mtx2Exec(mtx), query, args...)
}

// QueryRowContext runs sql.Tx.QueryRowContext on the tranactions in seperate Go routines.
// The first result is returned immediatly, regardless if that result has an error.
//
// Errors in sql.Tx.QueryRow are deferred untill scan and therefore opaque to this package.
// If you have a choice, stick with a regular QueryContext.
// This method is primarily included to implement boil.Executor.
func (mtx MultiTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return multiQueryRow(ctx, mtx2Exec(mtx), query, args...)
}

// QueryRow wrapper around sql.DB.QueryRow.
// Implements boil.Executor
// Since errors are defered untill row.Scan, this package cannot monitor such errors.
func (mtx MultiTx) QueryRow(query string, args ...interface{}) *sql.Row {
	return multiQueryRow(context.Background(), mtx2Exec(mtx), query, args...)
}