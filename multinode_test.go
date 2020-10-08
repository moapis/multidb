// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"testing"

	sm "github.com/DATA-DOG/go-sqlmock"
)

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
}

func TestMultiNode_Begin(t *testing.T) {
	t.Log("Begin wrapper")
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	mn := MultiNode(mdb.All())

	for _, mock := range mocks {
		mock.ExpectBegin()
	}

	m, err := mn.Begin()
	if err != nil {
		t.Error(err)
	}
	if len(m.tx) != 3 {
		t.Errorf("mtx.BeginTx() len of tx = %v, want %v", len(m.tx), 3)
	}
}

func BenchmarkMultiNode_txBeginners(b *testing.B) {
	mdb, _, err := multiTestConnect(benchmarkConns)
	if err != nil {
		b.Fatal(err)
	}
	mn := MultiNode(mdb.All())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mn.txBeginners()
	}
}
