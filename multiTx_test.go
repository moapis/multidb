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
	mn := MultiNode(mdb.All())
	for _, mock := range mocks {
		mock.ExpectBegin()
	}
	tx, err := mn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("ExecContext", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
		}
		got, err := tx.ExecContext(context.Background(), testQuery, 1)
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
		got, err := tx.Exec(testQuery, 1)
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
		r, err := tx.QueryContext(context.Background(), testQuery, 1)
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
		r, err := tx.Query(testQuery, 1)
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
		r := tx.QueryRowContext(context.Background(), testQuery, 1)
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
		r := tx.QueryRow(testQuery, 1)
		var got string
		if err = r.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("query() R = %v, want %v", got, want)
		}
	})
}

func TestMultiTx_Rollback(t *testing.T) {
	mdb, mocks, err := multiTestConnect([]string{"s", "t", "u"})
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
			t.Fatal(err)
		}
		for _, mock := range mocks {
			mock.ExpectRollback()
		}
		err = tx.Rollback()
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("1 Healty, two error", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectBegin()
		}
		tx, err := mn.BeginTx(context.Background(), nil)
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
	})
	t.Run("Different errors", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectBegin()
		}
		tx, err := mn.BeginTx(context.Background(), nil)
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
	})
}

func TestMultiTx_Commit(t *testing.T) {
	mdb, mocks, err := multiTestConnect([]string{"v", "w", "x"})
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
			t.Fatal(err)
		}
		for _, mock := range mocks {
			mock.ExpectCommit()
		}
		err = tx.Commit()
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("1 Healty, two error", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectBegin()
		}
		tx, err := mn.BeginTx(context.Background(), nil)
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
	})
	t.Run("Different errors", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectBegin()
		}
		tx, err := mn.BeginTx(context.Background(), nil)
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
	})
}
