package multidb

import (
	"context"
	"testing"

	sm "github.com/DATA-DOG/go-sqlmock"
	"github.com/volatiletech/sqlboiler/boil"
)

// Interface implementation checks
func _() boil.Executor          { return &Tx{} }
func _() boil.ContextExecutor   { return &Tx{} }
func _() boil.Transactor        { return &Tx{} }
func _() boil.ContextTransactor { return &Tx{} }

func beginTestTx() (*Tx, error) {
	n := newNode(defaultTestConfig(), testDSN, 10, 0, 0)
	if err := n.Open(); err != nil {
		return nil, err
	}
	mock.ExpectBegin()
	return n.Begin()
}

func TestTx_Rollback(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Error(err)
	}
}

func TestTx_Commit(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectCommit()
	if err := tx.Commit(); err != nil {
		t.Error(err)
	}
}

func TestTx_Exec(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(1, 1))

	got, err := tx.Exec(testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err := got.RowsAffected()
	if err != nil || i != 1 {
		t.Errorf("Node.Exec() Res = %v, want %v", i, 1)
	}
}

func TestTx_Query(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}

	want := "value"
	mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))

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
		t.Errorf("Node.Query() R = %v, want %v", got, want)
	}
}

func TestTx_QueryRow(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}

	want := "value"
	mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))

	r := tx.QueryRow(testQuery, 1)
	var got string
	if err := r.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("Node.QueryRow() R = %v, want %v", got, want)
	}
}

func TestTx_ExecContext(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(1, 1))

	got, err := tx.ExecContext(context.Background(), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err := got.RowsAffected()
	if err != nil || i != 1 {
		t.Errorf("Node.Exec() Res = %v, want %v", i, 1)
	}
}

func TestTx_QueryContext(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}

	want := "value"
	mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))

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
		t.Errorf("Node.Query() R = %v, want %v", got, want)
	}
}

func TestTx_QueryRowContext(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}

	want := "value"
	mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))

	r := tx.QueryRowContext(context.Background(), testQuery, 1)
	var got string
	if err := r.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("Node.QueryRow() R = %v, want %v", got, want)
	}
}
