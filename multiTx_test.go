package multidb

import (
	"context"
	"database/sql"
	"math/rand"
	"testing"
	"time"

	sm "github.com/DATA-DOG/go-sqlmock"
	"github.com/volatiletech/sqlboiler/v4/boil"
)

// Interface implementation checks
func _() boil.Executor          { return &MultiTx{} }
func _() boil.ContextExecutor   { return &MultiTx{} }
func _() boil.Transactor        { return &MultiTx{} }
func _() boil.ContextTransactor { return &MultiTx{} }

func Test_beginMultiTx(t *testing.T) {
	t.Log("All nodes healthy")
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn := MultiNode(mdb.All())

	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	txs, err := beginMultiTx(context.Background(), nil, mn.txBeginners()...)
	if err != nil {
		t.Error(err)
	}
	if len(txs) != 3 {
		t.Errorf("mtx.BeginTx() len of tx = %v, want %v", len(txs), 3)
	}

	t.Log("Healty delayed, two error")
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn = MultiNode(mdb.All())

	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectBegin().WillDelayFor(1 * time.Second)
		} else {
			mock.ExpectBegin().WillReturnError(sql.ErrConnDone)
		}
	}
	txs, err = beginMultiTx(context.Background(), nil, mn.txBeginners()...)
	if err != sql.ErrConnDone {
		t.Errorf("mtx.BeginTx() expected err: %v, got: %v", sql.ErrConnDone, err)
	}
	if len(txs) != 1 {
		t.Errorf("mtx.BeginTx() len of tx = %v, want %v", len(txs), 1)
	}

	t.Log("All same error")
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn = MultiNode(mdb.All())

	for _, mock := range mocks {
		mock.ExpectBegin().WillReturnError(sql.ErrConnDone)
	}
	txs, err = beginMultiTx(context.Background(), nil, mn.txBeginners()...)
	if err != sql.ErrConnDone {
		t.Errorf("Expected err: %v, got: %v", sql.ErrConnDone, err)
	}
	if txs != nil {
		t.Errorf("mtx.BeginTx() Res = %v, want %v", txs, nil)
	}

	t.Log("Different errors")
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn = MultiNode(mdb.All())

	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectBegin().WillReturnError(sql.ErrNoRows)
		} else {
			mock.ExpectBegin().WillReturnError(sql.ErrConnDone)
		}
	}
	txs, err = beginMultiTx(context.Background(), nil, mn.txBeginners()...)
	me, ok := err.(*MultiError)
	if !ok {
		t.Errorf("mtx.BeginTx() expected err type: %T, got: %T", MultiError{}, err)
	}
	if len(me.Errors) != 3 {
		t.Errorf("mtx.BeginTx() len of err = %v, want %v", len(me.Errors), 3)
	}
	if txs != nil {
		t.Errorf("mtx.BeginTx() Res = %v, want %v", txs, nil)
	}
}

// benchTxBeginner is simple (no-op) txBeginner implementation
type benchTxBeginner struct{}

func (*benchTxBeginner) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error) {
	return &sql.Tx{}, nil
}

func Benchmark_beginMultiTx(b *testing.B) {
	tbs := make([]txBeginner, benchmarkConns)

	for i := 0; i < benchmarkConns; i++ {
		tbs[i] = &benchTxBeginner{}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		beginMultiTx(context.Background(), nil, tbs...)
	}
}

func prepareTestTx() (*MultiTx, []sm.Sqlmock, error) {
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		return nil, nil, err
	}
	mn := MultiNode(mdb.All())
	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	tx, err := mn.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, nil, err
	}
	return tx, mocks, nil
}

// Simple tests for the wrapper methods
func TestMultiTx_General(t *testing.T) {
	t.Log("ExecContext")
	tx, mocks, err := prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectExec(testQuery).WithArgs(1).
			WillReturnResult(sm.NewResult(2, 3)).
			WillDelayFor(time.Duration(rand.Int63n(100)) * time.Millisecond)
		mock.ExpectRollback()
	}
	res, err := tx.ExecContext(context.Background(), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err := res.RowsAffected()
	if err != nil || i != 3 {
		t.Errorf("ExecContext() Res = %v, want %v", i, 3)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	t.Log("Exec")
	tx, mocks, err = prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectExec(testQuery).WithArgs(1).
			WillReturnResult(sm.NewResult(2, 3)).
			WillDelayFor(time.Duration(rand.Int63n(100)) * time.Millisecond)
		mock.ExpectRollback()
	}
	res, err = tx.Exec(testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err = res.RowsAffected()
	if err != nil || i != 3 {
		t.Errorf("Exec() Res = %v, want %v", i, 3)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	want := "value"

	t.Log("QueryContext")
	tx, mocks, err = prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).
			WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want)).
			WillDelayFor(time.Duration(rand.Int63n(100)) * time.Millisecond)
		mock.ExpectRollback()
	}
	rows, err := tx.QueryContext(context.Background(), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	rows.Next()
	var got string
	if err = rows.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("QueryContext() R = %v, want %v", got, want)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	t.Log("Query")
	tx, mocks, err = prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).
			WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want)).
			WillDelayFor(time.Duration(rand.Int63n(100)) * time.Millisecond)
		mock.ExpectRollback()
	}
	rows, err = tx.Query(testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	rows.Next()
	got = ""
	if err = rows.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("Query() R = %v, want %v", got, want)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	t.Log("QueryRowContext")
	tx, mocks, err = prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).
			WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want)).
			WillDelayFor(time.Duration(rand.Int63n(100)) * time.Millisecond)
		mock.ExpectRollback()
	}
	row := tx.QueryRowContext(context.Background(), testQuery, 1)
	got = ""
	if err = row.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("QueryRowContext() R = %v, want %v", got, want)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	t.Log("QueryRow")
	tx, mocks, err = prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).
			WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want)).
			WillDelayFor(time.Duration(rand.Int63n(100)) * time.Millisecond)
		mock.ExpectRollback()
	}
	row = tx.QueryRow(testQuery, 1)
	got = ""
	if err = row.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("QueryRow() R = %v, want %v", got, want)
	}

	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestMultiTx_Rollback(t *testing.T) {
	t.Log("All nodes healthy")
	tx, mocks, err := prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectRollback()
	}
	err = tx.Rollback()
	if err != nil {
		t.Error(err)
	}

	t.Log("1 Healty, two error")
	tx, mocks, err = prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectRollback()
		} else {
			mock.ExpectRollback().WillReturnError(sql.ErrConnDone)
		}
	}
	err = tx.Rollback()
	if err != sql.ErrConnDone {
		t.Errorf("mtx.Rollback() expected err: %v, got: %v", sql.ErrConnDone, err)
	}

	t.Log("Different errors")
	tx, mocks, err = prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectRollback().WillReturnError(sql.ErrNoRows)
		} else {
			mock.ExpectRollback().WillReturnError(sql.ErrConnDone)
		}
	}
	err = tx.Rollback()
	me, ok := err.(*MultiError)
	if !ok {
		t.Errorf("mtx.Rollback() expected err type: %T, got: %T", MultiError{}, err)
	}
	if len(me.Errors) != 3 {
		t.Errorf("mtx.Rollback() len of err = %v, want %v", len(me.Errors), 3)
	}
}

func TestMultiTx_Commit(t *testing.T) {
	t.Log("All nodes healthy")
	tx, mocks, err := prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectCommit()
	}
	err = tx.Commit()
	if err != nil {
		t.Error(err)
	}

	t.Log("1 Healty, two error")
	tx, mocks, err = prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectCommit()
		} else {
			mock.ExpectCommit().WillReturnError(sql.ErrConnDone)
		}
	}
	err = tx.Commit()
	if err != sql.ErrConnDone {
		t.Errorf("mtx.Commit() expected err: %v, got: %v", sql.ErrConnDone, err)
	}

	t.Log("Different errors")
	tx, mocks, err = prepareTestTx()
	if err != nil {
		t.Fatal(err)
	}
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectCommit().WillReturnError(sql.ErrNoRows)
		} else {
			mock.ExpectCommit().WillReturnError(sql.ErrConnDone)
		}
	}
	err = tx.Commit()
	me, ok := err.(*MultiError)
	if !ok {
		t.Errorf("mtx.Commit() expected err type: %T, got: %T", MultiError{}, err)
	}
	if len(me.Errors) != 3 {
		t.Errorf("mtx.Commit() len of err = %v, want %v", len(me.Errors), 3)
	}
}
