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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkMultiError(tt.errors); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MultiError.check() error = %v, want %v", got, tt.want)
			}
		})
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
		mdb.Add(NewNode(strconv.Itoa(i), db))
	}

	return mdb, mocks, nil
}

func Test_multiExec(t *testing.T) {
	t.Log("All nodes healthy")
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
	}
	got, err := multiExec(context.Background(), nil, nodes2Exec(mdb.All()), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err := got.RowsAffected()
	if err != nil || i != 3 {
		t.Errorf("multiExec() Res = %v, want %v", i, 3)
	}

	t.Log("Healty delayed, two error")
	mdb, mocks, err = multiTestConnect(defaultTestConns)
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

	wg := &sync.WaitGroup{}
	got, err = multiExec(context.Background(), wg, nodes2Exec(mdb.All()), testQuery, 1)
	wg.Wait()

	if err != nil {
		t.Error(err)
	}
	i, err = got.RowsAffected()
	if err != nil || i != 3 {
		t.Errorf("multiExec() Res = %v, want %v", i, 3)
	}

	t.Log("All same error")
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectExec(testQuery).WillReturnError(sql.ErrNoRows)
	}
	got, err = multiExec(context.Background(), nil, nodes2Exec(mdb.All()), testQuery, 1)
	if err != sql.ErrNoRows {
		t.Errorf("exec() expected err: %v, got: %v", sql.ErrNoRows, err)
	}
	if got != nil {
		t.Errorf("multiExec() Res = %v, want %v", got, nil)
	}

	t.Log("Different errors")
	mdb, mocks, err = multiTestConnect(defaultTestConns)
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
	got, err = multiExec(context.Background(), nil, nodes2Exec(mdb.All()), testQuery, 1)
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

func Test_multiQuery(t *testing.T) {
	t.Log("All nodes healthy")
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	want := "value"
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
	}
	rows, err := multiQuery(context.Background(), nil, nodes2Exec(mdb.All()), testQuery, 1)
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

	t.Log("Healty delayed, two error")
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectQuery(testQuery).WillDelayFor(1 * time.Second).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		} else {
			mock.ExpectQuery(testQuery).WillReturnError(sql.ErrConnDone)
		}
	}

	wg := &sync.WaitGroup{}

	rows, err = multiQuery(context.Background(), wg, nodes2Exec(mdb.All()), testQuery, 1)
	wg.Wait()

	if err != nil {
		t.Error(err)
	}
	rows.Next()
	got = ""
	if err = rows.Scan(&got); err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("multiQuery() R = %v, want %v", got, want)
	}

	t.Log("All same error")
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WillReturnError(sql.ErrNoRows)
	}
	rows, err = multiQuery(context.Background(), wg, nodes2Exec(mdb.All()), testQuery, 1)
	wg.Wait()

	if err != sql.ErrNoRows {
		t.Errorf("Expected err: %v, got: %v", sql.ErrNoRows, err)
	}
	if rows != nil {
		t.Errorf("multiQuery() Res = %v, want %v", rows, nil)
	}

	t.Log("Different errors")
	mdb, mocks, err = multiTestConnect(defaultTestConns)
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
	rows, err = multiQuery(context.Background(), nil, nodes2Exec(mdb.All()), testQuery, 1)
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

func Test_multiQueryRow(t *testing.T) {
	t.Log("All nodes healthy")
	mdb, mocks, err := multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	want := "value"
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
	}

	wg := &sync.WaitGroup{}
	row := multiQueryRow(context.Background(), wg, nodes2Exec(mdb.All()), testQuery, 1)
	wg.Wait()

	var got string
	if err = row.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("multiQueryRow() R = %v, want %v", got, want)
	}

	t.Log("All same error")
	mdb, mocks, err = multiTestConnect(defaultTestConns)
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WillReturnError(sql.ErrNoRows)
	}
	row = multiQueryRow(context.Background(), nil, nodes2Exec(mdb.All()), testQuery, 1)
	got = ""
	err = row.Scan(&got)
	if err != sql.ErrNoRows {
		t.Errorf("Expected err: %v, got: %v", sql.ErrNoRows, err)
	}
	if got != "" {
		t.Errorf("multiQueryRow() Res = %v, want %v", got, "")
	}
}

const benchmarkConns = 100

func Benchmark_nodes2Exec(b *testing.B) {
	mdb, _, err := multiTestConnect(benchmarkConns)
	if err != nil {
		b.Fatal(err)
	}

	nodes := mdb.nm.getList()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes2Exec(nodes)
	}
}

func Benchmark_mtx2Exec(b *testing.B) {
	mdb, mocks, err := multiTestConnect(benchmarkConns)
	if err != nil {
		b.Fatal(err)
	}

	for _, m := range mocks {
		m.ExpectBegin()
	}

	mtx, err := mdb.MultiTx(context.Background(), nil, benchmarkConns)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mtx2Exec(mtx.tx)
	}
}

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

func initBenchExecutors() []executor {
	ex := make([]executor, benchmarkConns)

	for i := 0; i < benchmarkConns; i++ {
		ex[i] = &benchExecutor{}
	}

	return ex
}

func Benchmark_multiExec(b *testing.B) {
	ex := initBenchExecutors()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiExec(context.Background(), nil, ex, "")
	}
}

func Benchmark_multiExec_wait(b *testing.B) {
	ex := initBenchExecutors()
	wg := &sync.WaitGroup{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiExec(context.Background(), wg, ex, "")
		wg.Wait()
	}
}

func Benchmark_multiQuery(b *testing.B) {
	ex := initBenchExecutors()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiQuery(context.Background(), nil, ex, "")
	}
}

func Benchmark_multiQuery_wait(b *testing.B) {
	ex := initBenchExecutors()
	wg := &sync.WaitGroup{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiQuery(context.Background(), wg, ex, "")
		wg.Wait()
	}
}

func Benchmark_multiQueryRow(b *testing.B) {
	ex := initBenchExecutors()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiQueryRow(context.Background(), nil, ex, "")
	}
}

func Benchmark_multiQueryRow_wait(b *testing.B) {
	ex := initBenchExecutors()
	wg := &sync.WaitGroup{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multiQueryRow(context.Background(), wg, ex, "")
		wg.Wait()
	}
}

/* Current Benchmark output:

go test -benchmem -bench .
goos: linux
goarch: amd64
pkg: github.com/moapis/multidb
Benchmark_beginMultiTx-8                   24188             46950 ns/op            5280 B/op        103 allocs/op
BenchmarkMultiNode_txBeginners-8         1382906               936 ns/op            1792 B/op          1 allocs/op
Benchmark_nodes2Exec-8                   1000000              1017 ns/op            1792 B/op          1 allocs/op
Benchmark_mtx2Exec-8                     1000000              1015 ns/op            1792 B/op          1 allocs/op
Benchmark_multiExec-8                      40836             28774 ns/op            3392 B/op          5 allocs/op
Benchmark_multiExec_wait-8                 36723             32793 ns/op            3392 B/op          5 allocs/op
Benchmark_multiQuery-8                     42198             29085 ns/op            3306 B/op          3 allocs/op
Benchmark_multiQuery_wait-8                26955             44723 ns/op            3292 B/op          3 allocs/op
Benchmark_multiQueryRow-8                  26910             44643 ns/op            1000 B/op          3 allocs/op
Benchmark_multiQueryRow_wait-8             26415             44659 ns/op            1000 B/op          3 allocs/op
PASS
ok      github.com/moapis/multidb       20.978s

*/
