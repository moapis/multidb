package multidb

import (
	"context"
	"database/sql"
	"testing"

	sm "github.com/DATA-DOG/go-sqlmock"
	"github.com/volatiletech/sqlboiler/boil"
)

// Interface implementation checks
func _() boil.Executor          { return MultiTx{} }
func _() boil.ContextExecutor   { return MultiTx{} }
func _() boil.Transactor        { return MultiTx{} }
func _() boil.ContextTransactor { return MultiTx{} }

func TestMultiTx_append(t *testing.T) {
	tests := []struct {
		name string
		mtx  MultiTx
		tx   *Tx
	}{
		{
			"Append",
			nil,
			&Tx{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mtx.append(tt.tx)
			if len(tt.mtx) != 1 {
				t.Errorf("mtx.append() len of mtx = %v, want %v", len(tt.mtx), 1)
			}
		})
	}
}

// Simple tests for the wrapper methods
func TestMultiTx_General(t *testing.T) {
	mdb, mocks, err := multiTestConnect([]string{"p", "q", "r"})
	if err != nil {
		t.Fatal(err)
	}
	defer mdb.Close()
	mn := MultiNode(mdb.All())
	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	tx, err := mn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ExecContext")
	for _, mock := range mocks {
		mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
	}
	res, err := tx.ExecContext(context.Background(), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err := res.RowsAffected()
	if err != nil || i != 3 {
		t.Errorf("ExecContext() Res = %v, want %v", i, 3)
	}

	t.Log("Exec")
	for _, mock := range mocks {
		mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
	}
	res, err = tx.Exec(testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err = res.RowsAffected()
	if err != nil || i != 3 {
		t.Errorf("Exec() Res = %v, want %v", i, 3)
	}

	want := "value"

	t.Log("QueryContext")
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
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

	t.Log("Query")
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
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

	t.Log("QueryRowContext")
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
	}
	row := tx.QueryRowContext(context.Background(), testQuery, 1)
	got = ""
	if err = row.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("QueryRowContext() R = %v, want %v", got, want)
	}

	t.Log("QueryRow")
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
	}
	row = tx.QueryRow(testQuery, 1)
	got = ""
	if err = row.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("QueryRow() R = %v, want %v", got, want)
	}
}

func TestMultiTx_Rollback(t *testing.T) {
	mdb, mocks, err := multiTestConnect([]string{"s", "t", "u"})
	if err != nil {
		t.Fatal(err)
	}
	defer mdb.Close()
	mn := MultiNode(mdb.All())

	t.Log("All nodes healthy")
	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	tx, err := mn.BeginTx(context.Background(), nil)
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
	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	tx, err = mn.BeginTx(context.Background(), nil)
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
	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	tx, err = mn.BeginTx(context.Background(), nil)
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
	me, ok := err.(MultiError)
	if !ok {
		t.Errorf("mtx.Rollback() expected err type: %T, got: %T", MultiError{}, err)
	}
	if len(me.Errors) != 3 {
		t.Errorf("mtx.Rollback() len of err = %v, want %v", len(me.Errors), 3)
	}
}

func TestMultiTx_Commit(t *testing.T) {
	mdb, mocks, err := multiTestConnect([]string{"v", "w", "x"})
	if err != nil {
		t.Fatal(err)
	}
	defer mdb.Close()
	mn := MultiNode(mdb.All())

	t.Log("All nodes healthy")
	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	tx, err := mn.BeginTx(context.Background(), nil)
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
	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	tx, err = mn.BeginTx(context.Background(), nil)
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
	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	tx, err = mn.BeginTx(context.Background(), nil)
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
	me, ok := err.(MultiError)
	if !ok {
		t.Errorf("mtx.Commit() expected err type: %T, got: %T", MultiError{}, err)
	}
	if len(me.Errors) != 3 {
		t.Errorf("mtx.Commit() len of err = %v, want %v", len(me.Errors), 3)
	}
}
