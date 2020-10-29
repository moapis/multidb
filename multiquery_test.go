// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	sm "github.com/DATA-DOG/go-sqlmock"
)

func TestMultiError_Error(t *testing.T) {
	tests := []struct {
		name   string
		errors []error
		want   string
	}{
		{
			"No cases",
			nil,
			"Unknown error",
		},
		{
			"Mutiple cases",
			[]error{
				errors.New("First"),
				errors.New("Second"),
				errors.New("Third"),
			},
			"Multiple errors: 1: First; 2: Second; 3: Third;",
		},
		{
			"With nil",
			[]error{
				nil,
				errors.New("First"),
				errors.New("Second"),
				errors.New("Third"),
			},
			"Multiple errors: 1: <nil>; 2: First; 3: Second; 4: Third;",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			me := MultiError{
				Errors: tt.errors,
			}
			if got := me.Error(); got != tt.want {
				t.Errorf("MultiError.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkMultiError(t *testing.T) {
	tests := []struct {
		name   string
		errors []error
		want   error
	}{
		{
			"No cases",
			nil,
			nil,
		},
		{
			"Different cases",
			[]error{
				errors.New("First"),
				errors.New("Second"),
				errors.New("Third"),
			},
			&MultiError{
				[]error{
					errors.New("First"),
					errors.New("Second"),
					errors.New("Third"),
				},
			},
		},
		{
			"Different NodeError cases",
			[]error{
				&NodeError{
					name:    "Node 1",
					wrapped: errors.New("First"),
				},
				&NodeError{
					name:    "Node 2",
					wrapped: errors.New("Second"),
				},
				&NodeError{
					name:    "Node 3",
					wrapped: errors.New("Third"),
				},
			},
			&MultiError{
				[]error{
					&NodeError{
						name:    "Node 1",
						wrapped: errors.New("First"),
					},
					&NodeError{
						name:    "Node 2",
						wrapped: errors.New("Second"),
					},
					&NodeError{
						name:    "Node 3",
						wrapped: errors.New("Third"),
					},
				},
			},
		},
		{
			"With nil",
			[]error{
				nil,
				errors.New("First"),
				errors.New("Second"),
				errors.New("Third"),
			},
			&MultiError{
				[]error{
					errors.New("First"),
					errors.New("Second"),
					errors.New("Third"),
				},
			},
		},
		{
			"Same cases",
			[]error{
				sql.ErrNoRows,
				sql.ErrNoRows,
				sql.ErrNoRows,
			},
			sql.ErrNoRows,
		},
		{
			"Same NodeError cases",
			[]error{
				&NodeError{
					name:    "Node 1",
					wrapped: sql.ErrNoRows,
				},
				&NodeError{
					name:    "Node 2",
					wrapped: sql.ErrNoRows,
				},
				&NodeError{
					name:    "Node 3",
					wrapped: sql.ErrNoRows,
				},
			},
			sql.ErrNoRows,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkMultiError(tt.errors); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MultiError.check() error = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeError(t *testing.T) {
	wrapped := errors.New("Spanac error")

	err := error(&NodeError{
		name:    "One",
		wrapped: wrapped,
	})

	want := "Node One: Spanac error"
	got := err.Error()
	if got != want {
		t.Errorf("NodeError.Error() = %s, want %s", got, want)
	}

	gotErr := errors.Unwrap(err)
	if gotErr != wrapped {
		t.Errorf("NodeError.Unwrap() = %v, want %v", gotErr, wrapped)
	}
}

const defaultTestConns = 3

func multiTestConnect(conns int) (*MultiDB, []sm.Sqlmock, error) {
	mocks := make([]sm.Sqlmock, conns)
	mdb := new(MultiDB)

	for i := 0; i < conns; i++ {
		db, mock, err := sm.New()
		if err != nil {
			return nil, nil, err
		}

		mocks[i] = mock
		mdb.Add(strconv.Itoa(i), db)
	}

	return mdb, mocks, nil
}

func Test_multiExec_healthy(t *testing.T) {
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
	}

	mn, err := mdb.MultiNode(0, testErrCallback(t))
	if err != nil {
		t.Fatal(err)
	}

	got, err := multiExec(context.Background(), nil, mn.nodes, testErrCallback(t), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err := got.RowsAffected()
	if err != nil || i != 3 {
		t.Errorf("multiExec() Res = %v, want %v", i, 3)
	}
}
func Test_multiExec_healthy_delayed(t *testing.T) {
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectExec(testQuery).WillDelayFor(1 * time.Second).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
		} else {
			mock.ExpectExec(testQuery).WillReturnError(sql.ErrConnDone)
		}
	}

	mn, err := mdb.MultiNode(0, testErrCallback(t))
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	got, err := multiExec(context.Background(), wg, mn.nodes, testErrCallback(t), testQuery, 1)
	wg.Wait()

	if err != nil {
		t.Error(err)
	}
	i, err := got.RowsAffected()
	if err != nil || i != 3 {
		t.Errorf("multiExec() Res = %v, want %v", i, 3)
	}

}

func Test_multiExec_error(t *testing.T) {
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectExec(testQuery).WillReturnError(sql.ErrNoRows)
	}

	mn, err := mdb.MultiNode(0, testErrCallback(t))
	if err != nil {
		t.Fatal(err)
	}

	got, err := multiExec(context.Background(), nil, mn.nodes, testErrCallback(t), testQuery, 1)
	if err != sql.ErrNoRows {
		t.Errorf("exec() expected err: %v, got: %v", sql.ErrNoRows, err)
	}
	if got != nil {
		t.Errorf("multiExec() Res = %v, want %v", got, nil)
	}
}

func Test_multiExec_MultiError(t *testing.T) {
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}

	mn, err := mdb.MultiNode(0, testErrCallback(t))
	if err != nil {
		t.Fatal(err)
	}

	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectExec(testQuery).WillReturnError(sql.ErrNoRows)
		} else {
			mock.ExpectExec(testQuery).WillReturnError(sql.ErrConnDone)
		}
	}
	got, err := multiExec(context.Background(), nil, mn.nodes, testErrCallback(t), testQuery, 1)
	if err == nil {
		t.Errorf("multiExec() expected err got: %v", err)
	}
	_, ok := err.(*MultiError)
	if !ok {
		t.Errorf("multiExec() expected err type: %T, got: %T", MultiError{}, err)
	}
	if got != nil {
		t.Errorf("multiExec() Res = %v, want %v", got, nil)
	}
}

func Test_multiQuery_healty(t *testing.T) {
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	want := "value"
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
	}

	mn, err := mdb.MultiNode(0, testErrCallback(t))
	if err != nil {
		t.Fatal(err)
	}

	rows, err := multiQuery(context.Background(), nil, mn.nodes, testErrCallback(t), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	rows.Next()
	var got string
	if err = rows.Scan(&got); err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("multiQuery() R = %v, want %v", got, want)
	}
}

func Test_multiQuery_healty_delayed(t *testing.T) {
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}

	want := "value"
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectQuery(testQuery).WillDelayFor(1 * time.Second).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		} else {
			mock.ExpectQuery(testQuery).WillReturnError(sql.ErrConnDone)
		}
	}

	mn, err := mdb.MultiNode(0, testErrCallback(t))
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}

	rows, err := multiQuery(context.Background(), wg, mn.nodes, testErrCallback(t), testQuery, 1)
	wg.Wait()

	if err != nil {
		t.Error(err)
	}
	rows.Next()

	var got string

	if err = rows.Scan(&got); err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("multiQuery() R = %v, want %v", got, want)
	}
}

func Test_multiQuery_error(t *testing.T) {
	t.Log("All same error")
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WillReturnError(sql.ErrNoRows)
	}

	mn, err := mdb.MultiNode(0, testErrCallback(t))
	if err != nil {
		t.Fatal(err)
	}

	rows, err := multiQuery(context.Background(), nil, mn.nodes, testErrCallback(t), testQuery, 1)

	if err != sql.ErrNoRows {
		t.Errorf("Expected err: %v, got: %v", sql.ErrNoRows, err)
	}
	if rows != nil {
		t.Errorf("multiQuery() Res = %v, want %v", rows, nil)
	}
}

func Test_multiQuery_MultiError(t *testing.T) {
	t.Log("Different errors")
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectQuery(testQuery).WillReturnError(sql.ErrNoRows)
		} else {
			mock.ExpectQuery(testQuery).WillReturnError(sql.ErrConnDone)
		}
	}

	mn, err := mdb.MultiNode(0, testErrCallback(t))
	if err != nil {
		t.Fatal(err)
	}

	rows, err := multiQuery(context.Background(), nil, mn.nodes, testErrCallback(t), testQuery, 1)
	if err == nil {
		t.Errorf("multiQuery() expected err got: %v", err)
	}
	_, ok := err.(*MultiError)
	if !ok {
		t.Errorf("multiQuery() expected err type: %T, got: %T", MultiError{}, err)
	}
	if rows != nil {
		t.Errorf("multiQuery() Res = %v, want %v", rows, nil)
	}
}

func Test_multiQueryRow_healthy(t *testing.T) {
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	want := "value"
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
	}

	mn, err := mdb.MultiNode(0, testErrCallback(t))
	if err != nil {
		t.Fatal(err)
	}

	row := multiQueryRow(context.Background(), nil, mn.nodes, testErrCallback(t), testQuery, 1)

	var got string
	if err = row.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("multiQueryRow() R = %v, want %v", got, want)
	}
}

func Test_multiQueryRow_healthy_delayed(t *testing.T) {
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	want := "value"
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectQuery(testQuery).WillDelayFor(1 * time.Second).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		} else {
			mock.ExpectQuery(testQuery).WillReturnError(sql.ErrConnDone)
		}
	}

	mn, err := mdb.MultiNode(0, testErrCallback(t))
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	row := multiQueryRow(context.Background(), &wg, mn.nodes, testErrCallback(t), testQuery, 1)
	wg.Wait()

	var got string
	if err = row.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("multiQueryRow() R = %v, want %v", got, want)
	}
}

func Test_multiQueryRow_error(t *testing.T) {
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WillReturnError(sql.ErrNoRows)
	}

	mn, err := mdb.MultiNode(0, testErrCallback(t))
	if err != nil {
		t.Fatal(err)
	}

	row := multiQueryRow(context.Background(), nil, mn.nodes, testErrCallback(t), testQuery, 1)

	var got string
	err = row.Scan(&got)
	if err != sql.ErrNoRows {
		t.Errorf("Expected err: %v, got: %v", sql.ErrNoRows, err)
	}
	if got != "" {
		t.Errorf("multiQueryRow() Res = %v, want %v", got, "")
	}
}

const benchmarkConns = 100

// benchExecutor is a simple (no-op) executor implementation
type benchExecutor struct{}

func (*benchExecutor) ExecContext(context.Context, string, ...interface{}) (sql.Result, error) {
	return nil, nil
}
func (*benchExecutor) QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error) {
	return nil, nil
}
func (*benchExecutor) QueryRowContext(context.Context, string, ...interface{}) *sql.Row {
	return nil
}

// benchErrCallbakc is no-op
func benchErrCallback(error) {}

func initBenchExecutors() map[string]executor {
	ex := make(map[string]executor, benchmarkConns)

	for i := 0; i < benchmarkConns; i++ {
		ex[strconv.Itoa(i)] = &benchExecutor{}
	}

	return ex
}

func Benchmark_multiExec(b *testing.B) {
	ex := initBenchExecutors()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiExec(context.Background(), nil, ex, benchErrCallback, "")
	}
}

func Benchmark_multiExec_wait(b *testing.B) {
	ex := initBenchExecutors()
	wg := &sync.WaitGroup{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiExec(context.Background(), wg, ex, benchErrCallback, "")
		wg.Wait()
	}
}

func Benchmark_multiQuery(b *testing.B) {
	ex := initBenchExecutors()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiQuery(context.Background(), nil, ex, benchErrCallback, "")
	}
}

func Benchmark_multiQuery_wait(b *testing.B) {
	ex := initBenchExecutors()
	wg := &sync.WaitGroup{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiQuery(context.Background(), wg, ex, benchErrCallback, "")
		wg.Wait()
	}
}

func Benchmark_multiQueryRow(b *testing.B) {
	ex := initBenchExecutors()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiQueryRow(context.Background(), nil, ex, benchErrCallback, "")
	}
}

func Benchmark_multiQueryRow_wait(b *testing.B) {
	ex := initBenchExecutors()
	wg := &sync.WaitGroup{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiQueryRow(context.Background(), wg, ex, benchErrCallback, "")
		wg.Wait()
	}
}

/* Current Benchmark output:

go test -benchmem -bench .
goos: linux
goarch: amd64
pkg: github.com/moapis/multidb
Benchmark_beginMultiTx-8                   19437             56681 ns/op           22453 B/op        106 allocs/op
BenchmarkMultiNode_txBeginners-8          148069              7682 ns/op            5457 B/op          4 allocs/op
Benchmark_multiExec-8                      38809             31069 ns/op            3392 B/op          5 allocs/op
Benchmark_multiExec_wait-8                 34560             34027 ns/op            3392 B/op          5 allocs/op
Benchmark_multiQuery-8                     38163             31505 ns/op            3314 B/op          4 allocs/op
Benchmark_multiQuery_wait-8                26208             47113 ns/op            3316 B/op          3 allocs/op
Benchmark_multiQueryRow-8                  26485             44842 ns/op            1000 B/op          3 allocs/op
Benchmark_multiQueryRow_wait-8             26640             45037 ns/op            1000 B/op          3 allocs/op
PASS
ok      github.com/moapis/multidb       17.723s

*/
