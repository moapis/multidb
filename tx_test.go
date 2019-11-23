package multidb

import (
	"context"
	"testing"
)

func beginTestTx() (*Tx, error) {
	n := newNode(testDBDriver, testDSN, 10, 0, 0)
	if err := n.Open(); err != nil {
		return nil, err
	}
	return n.Begin()
}

func TestTx_Rollback(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Error(err)
	}
}

func TestTx_Commit(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Error(err)
	}
}

func TestTx_Exec(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}
	r, err := tx.Exec("select $1;", 1)
	if err != nil {
		t.Error(err)
	}
	if r == nil {
		t.Errorf("Node.Query() R = %v, want %v", r, "Result")
	}
}

func TestTx_Query(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}
	r, err := tx.Query("select $1;", 1)
	if err != nil {
		t.Error(err)
	}
	if r == nil {
		t.Errorf("Node.Query() R = %v, want %v", r, "Result")
	}
}

func TestTx_QueryRow(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}
	r := tx.QueryRow("select $1;", 1)
	if r == nil {
		t.Errorf("Node.QueryRow() R = %v, want %v", r, "Result")
	}
}

func TestTx_ExecContext(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}
	r, err := tx.ExecContext(context.Background(), "select $1;", 1)
	if err != nil {
		t.Error(err)
	}
	if r == nil {
		t.Errorf("Node.ExecContext() R = %v, want %v", r, "Result")
	}
}

func TestTx_QueryContext(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}
	r, err := tx.QueryContext(context.Background(), "select $1;", 1)
	if err != nil {
		t.Error(err)
	}
	if r == nil {
		t.Errorf("Node.QueryContext() R = %v, want %v", r, "Result")
	}
}

func TestTx_QueryRowContext(t *testing.T) {
	tx, err := beginTestTx()
	if err != nil {
		t.Fatal(err)
	}
	r := tx.QueryRowContext(context.Background(), "select $1;", 1)
	if r == nil {
		t.Errorf("Node.QueryRowContext() R = %v, want %v", r, "Result")
	}
}
