// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
	"log"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	sm "github.com/DATA-DOG/go-sqlmock"
	"github.com/moapis/multidb/drivers"
	"github.com/volatiletech/sqlboiler/v4/boil"
)

const (
	testDBDriver = "sqlmock"
	testDSN      = "file::memory:"
	testQuery    = "select;"
)

type testError struct{}

func (testError) Error() string {
	return "I'm a test error"
}

// testConfig implements a naive drivers.Configurator
type testConfig struct {
	dn   string
	dsns []string
}

func (c testConfig) DriverName() string {
	return c.dn
}
func (c testConfig) DataSourceNames() []string {
	return c.dsns
}

func (c testConfig) MasterQuery() string {
	return "select true;"
}

func (c testConfig) WhiteList(err error) bool {
	_, ok := err.(testError)
	return ok
}

func defaultTestConfig() testConfig {
	return testConfig{
		testDBDriver,
		[]string{testDSN, testDSN, testDSN},
	}
}

// Interface implementation checks
func _() boil.Executor        { return &Node{} }
func _() boil.ContextExecutor { return &Node{} }

var (
	mock sm.Sqlmock
)

func TestMain(m *testing.M) {
	var err error
	if _, mock, err = sm.NewWithDSN(testDSN); err != nil {
		log.Fatal(err)
	}
	os.Exit(m.Run())
}

func Test_newNode(t *testing.T) {
	type args struct {
		conf           testConfig
		dataSourceName string
	}
	tests := []struct {
		name string
		args args
		want *Node
	}{
		{
			"SQLite3",
			args{
				conf:           defaultTestConfig(),
				dataSourceName: testDSN,
			},
			&Node{
				Configurator:   defaultTestConfig(),
				dataSourceName: testDSN,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newNode(tt.args.conf, tt.args.dataSourceName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNode_Open(t *testing.T) {
	type args struct {
		conf           drivers.Configurator
		dataSourceName string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"Success",
			args{
				conf:           defaultTestConfig(),
				dataSourceName: testDSN,
			},
			false,
		},
		{
			ErrAlreadyOpen,
			args{
				conf:           defaultTestConfig(),
				dataSourceName: testDSN,
			},
			true,
		},
		{
			"Bogus",
			args{
				conf:           testConfig{dn: "foo"},
				dataSourceName: "bar",
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newNode(tt.args.conf, tt.args.dataSourceName)
			if tt.name == ErrAlreadyOpen {
				if err := n.Open(); err != nil {
					t.Fatal(err)
				}
			}
			if err := n.Open(); (err != nil) != tt.wantErr {
				t.Errorf("Node.Open() error = %v, wantErr %v", err, tt.wantErr)
			}
			n.mtx.Lock()
			n.mtx.Unlock()
		})
	}
}

func TestNode_Close(t *testing.T) {
	node := newNode(defaultTestConfig(), testDSN)
	if err := node.Open(); err != nil {
		t.Fatal(err)
	}

	if err := node.Close(); err != nil {
		t.Errorf("Node.Close() error = %v, wantErr %v", err, false)
	}
}

func TestNode_Exec(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}

	mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(1, 1))

	got, err := n.Exec(testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err := got.RowsAffected()
	if err != nil || i != 1 {
		t.Errorf("Node.Exec() Res = %v, want %v", i, 1)
	}
}

func TestNode_Query(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}

	want := "value"
	mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))

	r, err := n.Query(testQuery, 1)
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

func TestNode_QueryRow(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}

	want := "value"
	mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))

	r := n.QueryRow(testQuery, 1)
	var got string
	if err := r.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("Node.QueryRow() R = %v, want %v", got, want)
	}
}

func TestNode_ExecContext(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}

	mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(1, 1))

	got, err := n.ExecContext(context.Background(), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err := got.RowsAffected()
	if err != nil || i != 1 {
		t.Errorf("Node.Exec() Res = %v, want %v", i, 1)
	}
}

func TestNode_QueryContext(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}

	want := "value"
	mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))

	r, err := n.QueryContext(context.Background(), testQuery, 1)
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

func TestNode_QueryRowContext(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}

	want := "value"
	mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))

	r := n.QueryRowContext(context.Background(), testQuery, 1)
	var got string
	if err := r.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("Node.QueryRow() R = %v, want %v", got, want)
	}
}

func TestNode_Begin(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()

	tx, err := n.Begin()
	if err != nil {
		t.Error(err)
	}
	if tx == nil {
		t.Errorf("Node.Begin() R = %v, want %v", tx, "TX")
	}
}

func TestNode_BeginTx(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()

	tx, err := n.BeginTx(context.Background(), nil)
	if err != nil {
		t.Error(err)
	}
	if tx == nil {
		t.Errorf("Node.BeginTx() R = %v, want %v", tx, "TX")
	}
}

func Test_newEntries(t *testing.T) {
	var nodes []*Node
	var exp entries
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		db, mock, err := sm.New()
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(10)
		var mu sync.RWMutex
		for n := 0; n < i; n++ {
			mu.Lock()
			mock.ExpectExec(testQuery).WillDelayFor(time.Second).WillReturnResult(sm.NewResult(2, 2))
			mu.Unlock()
		}
		wg.Add(i)
		for n := 0; n < i; n++ {
			// Increase InUse counters
			go func() {
				mu.RLock()
				wg.Done()
				db.Exec(testQuery)
				mu.RUnlock()
			}()
		}
		node := &Node{DB: db}
		exp = append(exp, entry{
			node,
			float32(i) / 10.0,
		})
		nodes = append(nodes, node) // nil to add some garbage
	}
	wg.Wait() // Allow for the exec go-routines to fire.
	got := newEntries(nodes)
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("newEntries() = \n%v\nwant\n%v", got, exp)
	}
}

func Test_entries_sortAndSlice(t *testing.T) {
	nodes := make([]*Node, 10)
	ent := entries(make([]entry, 10))
	for i := 0; i < 10; i++ {
		nodes[i] = &Node{}
		ent[9-i].node = nodes[i]
		ent[9-i].factor = float32(i)
	}
	tests := []struct {
		name string
		ent  entries
		max  int
		want []*Node
	}{
		{
			"nil",
			nil,
			10,
			[]*Node{},
		},
		{
			"Reversed list",
			ent,
			0,
			nodes,
		},
		{
			"Limited list",
			ent,
			3,
			nodes[:3],
		},
		{
			"Shorter list",
			ent[:3],
			10,
			nodes[:3],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ent.sortAndSlice(tt.max); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("entries.sortAndSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_availableNodes(t *testing.T) {
	exp := make([]*Node, 10)
	arg := make([]*Node, 10)
	for i := 0; i < 10; i++ {
		n := newNode(defaultTestConfig(), testDSN)
		if err := n.Open(); err != nil {
			t.Fatal(err)
		}
		exp[i] = n
		arg[i] = n
	}
	type args struct {
		nodes []*Node
		max   int
	}
	tests := []struct {
		name    string
		args    args
		want    []*Node
		wantErr bool
	}{
		{
			"nil",
			args{
				nil,
				1,
			},
			nil,
			true,
		},
		{
			"Limited list",
			args{
				arg,
				4,
			},
			exp[:4],
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := availableNodes(tt.args.nodes, tt.args.max)
			if (err != nil) != tt.wantErr {
				t.Errorf("availableNodes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("availableNodes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readOnlyOpts(t *testing.T) {
	tests := []struct {
		name string
		opts *sql.TxOptions
		want *sql.TxOptions
	}{
		{
			"Nil opts",
			nil,
			&sql.TxOptions{ReadOnly: true},
		},
		{
			"ReadOnly true",
			&sql.TxOptions{ReadOnly: true},
			&sql.TxOptions{ReadOnly: true},
		},
		{
			"Isolation level",
			&sql.TxOptions{Isolation: sql.LevelLinearizable},
			&sql.TxOptions{Isolation: sql.LevelLinearizable, ReadOnly: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readOnlyOpts(tt.opts); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readOnlyOpts() = %v, want %v", got, tt.want)
			}
		})
	}
}
