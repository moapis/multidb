package multidb

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sm "github.com/DATA-DOG/go-sqlmock"
	"github.com/volatiletech/sqlboiler/boil"
)

// Interface implementation checks
func _() boil.Executor        { return MultiNode{} }
func _() boil.ContextExecutor { return MultiNode{} }

// Simple tests for the wrapper methods
func TestMultiNode_General(t *testing.T) {
	mdb, mocks, err := multiTestConnect([]string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	defer mdb.Close()
	mn := MultiNode(mdb.All())

	t.Log("ExecContext")
	for _, mock := range mocks {
		mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
	}
	res, err := mn.ExecContext(context.Background(), testQuery, 1)
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
	res, err = mn.Exec(testQuery, 1)
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
	rows, err := mn.QueryContext(context.Background(), testQuery, 1)
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
	rows, err = mn.Query(testQuery, 1)
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
	row := mn.QueryRowContext(context.Background(), testQuery, 1)
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
	row = mn.QueryRow(testQuery, 1)
	got = ""
	if err = row.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("QueryRow() R = %v, want %v", got, want)
	}
}

func TestMultiNode_BeginTx(t *testing.T) {
	mdb, mocks, err := multiTestConnect([]string{"d", "e", "f"})
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
		t.Error(err)
	}
	if len(tx) != 3 {
		t.Errorf("mtx.BeginTx() len of tx = %v, want %v", len(tx), 3)
	}

	t.Log("Healty delayed, two error")
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectBegin().WillDelayFor(1 * time.Second)
		} else {
			mock.ExpectBegin().WillReturnError(sql.ErrConnDone)
		}
	}
	tx, err = mn.BeginTx(context.Background(), nil)
	if err != sql.ErrConnDone {
		t.Errorf("mtx.BeginTx() expected err: %v, got: %v", sql.ErrConnDone, err)
	}
	if len(tx) != 1 {
		t.Errorf("mtx.BeginTx() len of tx = %v, want %v", len(tx), 1)
	}

	t.Log("All same error")
	for _, mock := range mocks {
		mock.ExpectBegin().WillReturnError(sql.ErrConnDone)
	}
	tx, err = mn.BeginTx(context.Background(), nil)
	if err != sql.ErrConnDone {
		t.Errorf("Expected err: %v, got: %v", sql.ErrConnDone, err)
	}
	if tx != nil {
		t.Errorf("mtx.BeginTx() Res = %v, want %v", tx, nil)
	}

	t.Log("Different errors")
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectBegin().WillReturnError(sql.ErrNoRows)
		} else {
			mock.ExpectBegin().WillReturnError(sql.ErrConnDone)
		}
	}
	tx, err = mn.BeginTx(context.Background(), nil)
	me, ok := err.(MultiError)
	if !ok {
		t.Errorf("mtx.BeginTx() expected err type: %T, got: %T", MultiError{}, err)
	}
	if len(me.Errors) != 3 {
		t.Errorf("mtx.BeginTx() len of err = %v, want %v", len(me.Errors), 3)
	}
	if tx != nil {
		t.Errorf("mtx.BeginTx() Res = %v, want %v", tx, nil)
	}

	t.Log("Expire context")
	for _, mock := range mocks {
		mock.ExpectBegin().WillDelayFor(1 * time.Second)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	tx, err = mn.BeginTx(ctx, nil)
	if err != context.DeadlineExceeded {
		t.Errorf("mtx.BeginTx() expected err: %v, got: %v", context.DeadlineExceeded, err)
	}
	if tx != nil {
		t.Errorf("mtx.BeginTx() Res = %v, want %v", tx, nil)
	}

	t.Log("Begin wrapper")
	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	tx, err = mn.Begin()
	if err != nil {
		t.Error(err)
	}
	if len(tx) != 3 {
		t.Errorf("mtx.BeginTx() len of tx = %v, want %v", len(tx), 3)
	}
}
