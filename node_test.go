// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

func Test_newNodeStats(t *testing.T) {
	type args struct {
		statsLen int
		maxFails int
	}
	tt := struct {
		name string
		args args
		want nodeStats
	}{
		"newNodeStats",
		args{
			statsLen: 1000,
			maxFails: 50,
		},
		nodeStats{
			maxFails: 50,
			fails:    make([]bool, 1000),
		},
	}

	t.Run(tt.name, func(t *testing.T) {
		got := newNodeStats(tt.args.statsLen, tt.args.maxFails)
		if !reflect.DeepEqual(&got, &tt.want) {
			t.Errorf("newNodeStats() = %v, want %v", &got, &tt.want)
		}
		got.mtx.Lock()
		got.mtx.Unlock()
	})
}

func Test_nodeStats_reset(t *testing.T) {
	got := newNodeStats(10, 50)
	for i := range got.fails {
		got.fails[i] = true
	}
	got.reset()
	want := newNodeStats(10, 50)
	if !reflect.DeepEqual(&got, &want) {
		t.Errorf("newNodeStats() = %v, want %v", &got, &want)
	}
	got.mtx.Lock()
	got.mtx.Unlock()
}

func Test_nodeStats_failed(t *testing.T) {
	confs := []int{0, 2, 3, 4, -1}
	type args struct {
		state bool
	}
	tests := []struct {
		name  string
		args  args
		wants []bool
	}{
		{
			"0/4",
			args{false},
			[]bool{false, false, false, false, false},
		},
		{
			"Up to 1/4",
			args{true},
			[]bool{true, false, false, false, false},
		},
		{
			"Up to 2/4",
			args{true},
			[]bool{true, false, false, false, false},
		},
		{
			"Up to 3/4",
			args{true},
			[]bool{true, true, false, false, false},
		},
		{
			"Up to 4/4 and wrap",
			args{true},
			[]bool{true, true, true, false, false},
		},
		{
			"Down to 3/4",
			args{false},
			[]bool{true, true, false, false, false},
		},
		{
			"Down to 2/4",
			args{false},
			[]bool{true, false, false, false, false},
		},
	}
	// Run with 0 conf, to check for panic
	s := newNodeStats(0, 0)
	got := s.failed(true)
	if got != false {
		t.Errorf("nodeStats.failed() = %v, want %v", got, false)
	}

	for n, c := range confs {
		s := newNodeStats(4, c)
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%d: %v", c, tt.name), func(t *testing.T) {
				got := s.failed(tt.args.state)
				if got != tt.wants[n] {
					t.Errorf("nodeStats.failed() = %v, want %v", got, tt.wants[n])
				}
				s.mtx.Lock()
				s.mtx.Unlock()
			})
		}
	}
}

// sql.Open(sqlite3,testDSN

func Test_newNode(t *testing.T) {
	type args struct {
		conf           testConfig
		dataSourceName string
		statsLen       int
		maxFails       int
		reconnectWait  time.Duration
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
				statsLen:       1000,
				maxFails:       22,
				reconnectWait:  5 * time.Second,
			},
			&Node{
				nodeStats: nodeStats{
					maxFails: 22,
					fails:    make([]bool, 1000),
				},
				Configurator:   defaultTestConfig(),
				dataSourceName: testDSN,
				reconnectWait:  5 * time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newNode(tt.args.conf, tt.args.dataSourceName, tt.args.statsLen, tt.args.maxFails, tt.args.reconnectWait); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNode_Open(t *testing.T) {
	type args struct {
		conf               drivers.Configurator
		dataSourceName     string
		statsLen, maxFails int
		reconnectWait      time.Duration
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
				statsLen:       1000,
				maxFails:       22,
				reconnectWait:  5 * time.Second,
			},
			false,
		},
		{
			ErrAlreadyOpen,
			args{
				conf:           defaultTestConfig(),
				dataSourceName: testDSN,
				statsLen:       1000,
				maxFails:       22,
				reconnectWait:  5 * time.Second,
			},
			true,
		},
		{
			"Bogus",
			args{
				conf:           testConfig{dn: "foo"},
				dataSourceName: "bar",
				statsLen:       1000,
				maxFails:       22,
				reconnectWait:  5 * time.Second,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newNode(tt.args.conf, tt.args.dataSourceName, tt.args.statsLen, tt.args.maxFails, tt.args.reconnectWait)
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
	tests := []struct {
		name    string
		node    *Node
		wantErr bool
	}{
		{
			"Open",
			newNode(defaultTestConfig(), testDSN, 1000, 22, 0),
			false,
		},
		{
			"Closed",
			newNode(defaultTestConfig(), testDSN, 1000, 22, 0),
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "Open" {
				if err := tt.node.Open(); err != nil {
					t.Fatal(err)
				}
			}
			if err := tt.node.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Node.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
		tt.node.mtx.Lock()
		tt.node.mtx.Unlock()
	}
}

func TestNode_setReconnecting(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN, 1000, 22, 0)
	tests := []bool{true, true, false, true}
	for _, b := range tests {
		n.setReconnecting(b)
		if n.reconnecting != b {
			t.Errorf("Node.setReconnecting() got = %v, want %v", n.reconnecting, b)
		}
	}
}

func TestNode_reconnect(t *testing.T) {
	opened := newNode(defaultTestConfig(), testDSN, 1000, 22, 5*time.Millisecond)
	if err := opened.Open(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		node   *Node
		wantDB bool
	}{
		{
			"Close to open",
			newNode(defaultTestConfig(), testDSN, 1000, 22, 5*time.Millisecond),
			true,
		},
		{
			"Already open",
			opened,
			true,
		},
		{
			"No reconnect",
			newNode(defaultTestConfig(), testDSN, 1000, 22, 0),
			false,
		},
		{
			"Loop", // Will always fail
			newNode(testConfig{dn: "foo"}, "woogie", 1000, 22, 5*time.Millisecond),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if tt.name == "Loop" {
				// Break the loop after 1 sec.
				go func() {
					time.Sleep(time.Second)
					cancel()
				}()
			}
			tt.node.reconnect(ctx)
			if (tt.node.DB != nil) != tt.wantDB {
				t.Errorf("Node.Open() DB = %v, wantDB %v", tt.node.DB, tt.wantDB)
			}
		})
	}
}

func TestNode_Reconnecting(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{
			"true",
			true,
		},
		{
			"false",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &Node{
				reconnecting: tt.want,
			}
			if got := n.Reconnecting(); got != tt.want {
				t.Errorf("Node.Reconnecting() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNode_InUse(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN, 1000, 22, 0)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}
	use := n.InUse()
	if use != 0 {
		t.Errorf("Node.InUse() Use = %v, want %d", use, 0)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	use = n.InUse()
	if use != -1 {
		t.Errorf("Node.InUse() Use = %v, want %d", use, -1)
	}
}

func TestNode_Err(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN, 1000, 22, 0)
	n.setErr(errors.New(ErrAlreadyOpen))
	err := n.ConnErr()
	if err == nil || err.Error() != ErrAlreadyOpen {
		t.Errorf("Node.ConnErr() Err = %v, want %v", err, ErrAlreadyOpen)
	}

	n.mtx.Lock()
	n.mtx.Unlock()
}

func TestNode_checkFailed(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN, 10, 5, 0)
	if err := n.Open(); err != nil {
		t.Fatal(n)
	}
	for i := 0; i < 6; i++ {
		if n.DB == nil {
			t.Errorf("Node.checkFailed() DB = %v, want %v", n.DB, "DB")
		}
		n.checkFailed(true)
	}
	if n.DB != nil {
		t.Errorf("Node.checkFailed() DB = %v, want %v", n.DB, nil)
	}
}

func TestNode_CheckErr(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN, 10, 1, 0)
	if err := n.Open(); err != nil {
		t.Fatal(n)
	}

	whiteErrs := []error{
		nil,
		sql.ErrNoRows,
		sql.ErrTxDone,
		testError{},
	}
	// white listed errors, should not close connection
	for _, w := range whiteErrs {
		err := n.CheckErr(w)
		time.Sleep(time.Millisecond)
		if err != w {
			t.Errorf("Node.CheckErr() Err = %v, want %v", err, w)
		}

		n.mtx.RLock()
		if n.connErr != nil {
			t.Errorf("Node.CheckErr() connErr = %v, want %v", n.connErr, nil)
		}
		if n.DB == nil {
			t.Errorf("Node.CheckErr() DB = %v, want %v", n.DB, "DB")
		}
		n.mtx.RUnlock()
	}

	// First connection error, should not close connection
	err := n.CheckErr(sql.ErrConnDone)
	time.Sleep(time.Millisecond)
	if err != sql.ErrConnDone {
		t.Errorf("Node.CheckErr() Err = %v, want %v", err, sql.ErrConnDone)
	}

	n.mtx.RLock()
	if n.connErr != sql.ErrConnDone {
		t.Errorf("Node.CheckErr() connErr = %v, want %v", n.connErr, sql.ErrConnDone)
	}
	if n.DB == nil {
		t.Errorf("Node.CheckErr() DB = %v, want %v", n.DB, "DB")
	}
	n.mtx.RUnlock()

	// Here the connection should be closed
	err = n.CheckErr(sql.ErrConnDone)
	time.Sleep(time.Millisecond)
	if err != sql.ErrConnDone {
		t.Errorf("Node.CheckErr() Err = %v, want %v", err, sql.ErrConnDone)
	}

	n.mtx.RLock()
	if n.connErr != sql.ErrConnDone {
		t.Errorf("Node.CheckErr() connErr = %v, want %v", n.connErr, sql.ErrConnDone)
	}
	if n.DB != nil {
		t.Errorf("Node.CheckErr() DB = %v, want %v", n.DB, nil)
	}
	n.mtx.RUnlock()
}

func TestNode_Exec(t *testing.T) {
	n := newNode(defaultTestConfig(), testDSN, 10, 0, 0)
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
	n := newNode(defaultTestConfig(), testDSN, 10, 0, 0)
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
	n := newNode(defaultTestConfig(), testDSN, 10, 0, 0)
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
	n := newNode(defaultTestConfig(), testDSN, 10, 0, 0)
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
	n := newNode(defaultTestConfig(), testDSN, 10, 0, 0)
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
	n := newNode(defaultTestConfig(), testDSN, 10, 0, 0)
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
	n := newNode(defaultTestConfig(), testDSN, 10, 0, 0)
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
	n := newNode(defaultTestConfig(), testDSN, 10, 0, 0)
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
		nodes = append(nodes, node, nil, &Node{DB: nil}) // nil to add some garbage
	}
	wg.Wait() // Allow for the exec go-routines to fire.
	got := newEntries(nodes)
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("newEntries() = %v, want %v", got, exp)
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
		n := newNode(defaultTestConfig(), testDSN, 10, 0, 0)
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
