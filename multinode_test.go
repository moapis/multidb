package multidb

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sm "github.com/DATA-DOG/go-sqlmock"
	"github.com/volatiletech/sqlboiler/v4/boil"
)

// Interface implementation checks
func _() boil.Executor        { return MultiNode{} }
func _() boil.ContextExecutor { return MultiNode{} }

// Simple tests for the wrapper methods
func TestMultiNode_General(t *testing.T) {
	t.Log("ExecContext")
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn := MultiNode(mdb.All())

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
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn = MultiNode(mdb.All())

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
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn = MultiNode(mdb.All())

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
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn = MultiNode(mdb.All())

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
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn = MultiNode(mdb.All())

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
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn = MultiNode(mdb.All())

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
	t.Log("All nodes healthy")
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn := MultiNode(mdb.All())

	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	m, err := mn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Error(err)
	}
	if len(m.tx) != 3 {
		t.Errorf("mtx.BeginTx() len of tx = %v, want %v", len(m.tx), 3)
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
	m, err = mn.BeginTx(context.Background(), nil)
	if err != sql.ErrConnDone {
		t.Errorf("mtx.BeginTx() expected err: %v, got: %v", sql.ErrConnDone, err)
	}
	if len(m.tx) != 1 {
		t.Errorf("mtx.BeginTx() len of tx = %v, want %v", len(m.tx), 1)
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
	m, err = mn.BeginTx(context.Background(), nil)
	if err != sql.ErrConnDone {
		t.Errorf("Expected err: %v, got: %v", sql.ErrConnDone, err)
	}
	if m != nil {
		t.Errorf("mtx.BeginTx() Res = %v, want %v", m.tx, nil)
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
	m, err = mn.BeginTx(context.Background(), nil)
	me, ok := err.(MultiError)
	if !ok {
		t.Errorf("mtx.BeginTx() expected err type: %T, got: %T", MultiError{}, err)
	}
	if len(me.Errors) != 3 {
		t.Errorf("mtx.BeginTx() len of err = %v, want %v", len(me.Errors), 3)
	}
	if m != nil {
		t.Errorf("mtx.BeginTx() Res = %v, want %v", m.tx, nil)
	}

	t.Log("Begin wrapper")
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn = MultiNode(mdb.All())

	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	m, err = mn.Begin()
	if err != nil {
		t.Error(err)
	}
	if len(m.tx) != 3 {
		t.Errorf("mtx.BeginTx() len of tx = %v, want %v", len(m.tx), 3)
	}
}
