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
	mn := MultiNode(mdb.All())

	t.Run("ExecContext", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
		}
		got, err := mn.ExecContext(context.Background(), testQuery, 1)
		if err != nil {
			t.Error(err)
		}
		i, err := got.RowsAffected()
		if err != nil || i != 3 {
			t.Errorf("exec() Res = %v, want %v", i, 3)
		}
	})
	t.Run("Exec", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
		}
		got, err := mn.Exec(testQuery, 1)
		if err != nil {
			t.Error(err)
		}
		i, err := got.RowsAffected()
		if err != nil || i != 3 {
			t.Errorf("exec() Res = %v, want %v", i, 3)
		}
	})
	want := "value"
	t.Run("QueryContext", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		}
		r, err := mn.QueryContext(context.Background(), testQuery, 1)
		if err != nil {
			t.Error(err)
		}
		r.Next()
		var got string
		if err = r.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("query() R = %v, want %v", got, want)
		}
	})
	t.Run("Query", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		}
		r, err := mn.Query(testQuery, 1)
		if err != nil {
			t.Error(err)
		}
		r.Next()
		var got string
		if err = r.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("query() R = %v, want %v", got, want)
		}
	})
	t.Run("QueryRowContext", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		}
		r := mn.QueryRowContext(context.Background(), testQuery, 1)
		var got string
		if err = r.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("query() R = %v, want %v", got, want)
		}
	})
	t.Run("QueryRow", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		}
		r := mn.QueryRow(testQuery, 1)
		var got string
		if err = r.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("query() R = %v, want %v", got, want)
		}
	})
}

func TestMultiNode_BeginTx(t *testing.T) {
	mdb, mocks, err := multiTestConnect([]string{"d", "e", "f"})
	if err != nil {
		t.Fatal(err)
	}
	mn := MultiNode(mdb.All())

	t.Run("All nodes healthy", func(t *testing.T) {
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
	})
	t.Run("Healty delayed, two error", func(t *testing.T) {
		for i, mock := range mocks {
			if i == 0 {
				mock.ExpectBegin().WillDelayFor(1 * time.Second)
			} else {
				mock.ExpectBegin().WillReturnError(sql.ErrConnDone)
			}
		}
		tx, err := mn.BeginTx(context.Background(), nil)
		if err != sql.ErrConnDone {
			t.Errorf("mtx.BeginTx() expected err: %v, got: %v", sql.ErrConnDone, err)
		}
		if len(tx) != 1 {
			t.Errorf("mtx.BeginTx() len of tx = %v, want %v", len(tx), 1)
		}
	})
	t.Run("All same error", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectBegin().WillReturnError(sql.ErrConnDone)
		}
		tx, err := mn.BeginTx(context.Background(), nil)
		if err != sql.ErrConnDone {
			t.Errorf("Expected err: %v, got: %v", sql.ErrConnDone, err)
		}
		if tx != nil {
			t.Errorf("mtx.BeginTx() Res = %v, want %v", tx, nil)
		}
	})
	t.Run("Different errors", func(t *testing.T) {
		for i, mock := range mocks {
			if i == 0 {
				mock.ExpectBegin().WillReturnError(sql.ErrNoRows)
			} else {
				mock.ExpectBegin().WillReturnError(sql.ErrConnDone)
			}
		}
		tx, err := mn.BeginTx(context.Background(), nil)
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
	})
	time.Sleep(10 * time.Millisecond)
	t.Run("Expire context", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectBegin().WillDelayFor(1 * time.Second)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		tx, err := mn.BeginTx(ctx, nil)
		if err != context.DeadlineExceeded {
			t.Errorf("mtx.BeginTx() expected err: %v, got: %v", context.DeadlineExceeded, err)
		}
		if tx != nil {
			t.Errorf("mtx.BeginTx() Res = %v, want %v", tx, nil)
		}
	})
	t.Run("Begin wrapper", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectBegin()
		}
		tx, err := mn.Begin()
		if err != nil {
			t.Error(err)
		}
		if len(tx) != 3 {
			t.Errorf("mtx.BeginTx() len of tx = %v, want %v", len(tx), 3)
		}
	})
}
