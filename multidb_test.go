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
	"math"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	sm "github.com/DATA-DOG/go-sqlmock"
)

const (
	testDBDriver = "sqlmock"
	testDSN      = "file::memory:"
	testQuery    = "select;"
)

var (
	mock sm.Sqlmock
	mdb  *MultiDB
)

func TestMain(m *testing.M) {
	var err error
	if _, mock, err = sm.NewWithDSN(testDSN); err != nil {
		log.Fatal(err)
	}
	os.Exit(m.Run())
}

var errTest = errors.New("A test error")

func TestNoMasterErr(t *testing.T) {
	err := error(&NoMasterErr{errTest})

	want := fmt.Sprintf(errNoMaster, errTest)
	got := err.Error()
	if got != want {
		t.Errorf("NoMasterErr.Error() = %s, want %s", got, want)
	}

	target := errors.Unwrap(err)
	if target != errTest {
		t.Errorf("NoMasterErr.Unwrap() = %v, want %v", target, errTest)
	}
}

func Test_useFactor(t *testing.T) {
	stats := new(sql.DB).Stats()

	tests := []struct {
		name  string
		stats *sql.DBStats
		want  float64
	}{
		{
			"empty DB",
			&stats,
			0,
		},
		{
			"NaN",
			&sql.DBStats{
				MaxOpenConnections: 0,
				InUse:              0,
			},
			0,
		},
		{
			"Inf",
			&sql.DBStats{
				MaxOpenConnections: 0,
				InUse:              1,
			},
			math.Inf(1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := useFactor(tt.stats); got != tt.want {
				t.Errorf("useFactor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dbSorter(t *testing.T) {
	sorter := dbSorter{
		stats: []*sql.DBStats{
			// NaN
			{
				MaxOpenConnections: 0,
				InUse:              0,
			},
			{
				MaxOpenConnections: 0,
				InUse:              1,
			},
			{
				MaxOpenConnections: 10,
				InUse:              0,
			},
		},
		names: []string{
			"one",
			"two",
			"three",
		},
	}

	if ln := sorter.Len(); ln != len(sorter.stats) {
		t.Errorf("dbSorter.Len = %v, want %v", ln, len(sorter.stats))
	}

	if less := sorter.Less(1, 2); less {
		t.Errorf("dbSorter.Less = %v, want %v", less, false)
	}

	want := dbSorter{
		stats: []*sql.DBStats{
			sorter.stats[1],
			sorter.stats[0],
			sorter.stats[2],
		},
		names: []string{
			sorter.names[1],
			sorter.names[0],
			sorter.names[2],
		},
	}

	sorter.Swap(0, 1)

	if !reflect.DeepEqual(sorter, want) {
		t.Errorf("dbSorter.Swap = \n%v\nwant:\n%v", sorter, want)
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

func TestMultiDB_Close(t *testing.T) {
	mdb := &MultiDB{}

	db, mock, err := sm.New()
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectClose()

	mdb.Add("ok", db)

	db, mock, err = sm.New()
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectClose().WillReturnError(sql.ErrConnDone)

	mdb.Add("err", db)

	err = mdb.Close()
	if !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("mdb.Close() err = %v, want %v", err, sql.ErrConnDone)
	}
}

func TestMultiDB_Add_Delete(t *testing.T) {
	mdb := &MultiDB{
		nodes: map[string]*sql.DB{
			"one":   new(sql.DB),
			"two":   new(sql.DB),
			"three": new(sql.DB),
		},
	}

	mdb.Add("four", new(sql.DB))
	mdb.Delete("one", "two")

	mdb.nmu.RLock()
	mdb.nmu.RUnlock()
}

// Includes test for mdb.Master()
func TestMultiDB_selectMaster(t *testing.T) {
	mocks := map[string]sm.Sqlmock{"master": nil, "slave": nil, "borked": nil, "errored": nil}

	mdb := &MultiDB{
		MasterFunc: IsMaster(testMasterQuery),
	}

	for k := range mocks {
		var (
			db  *sql.DB
			err error
		)

		db, mocks[k], err = sm.New()
		if err != nil {
			t.Fatal(err)
		}

		mdb.Add(k, db)
	}

	mocks["master"].ExpectQuery(testMasterQuery).WillDelayFor(100 * time.Millisecond).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(true))
	mocks["slave"].ExpectQuery(testMasterQuery).WillDelayFor(50 * time.Millisecond).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(false))
	mocks["borked"].ExpectQuery(testMasterQuery).WillDelayFor(time.Second)
	mocks["errored"].ExpectQuery(testMasterQuery).WillReturnError(sql.ErrConnDone)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Execute twice. First time will run mdb.selectMaster.
	// Second time will return directly the mdb.master object
	for i := 0; i < 2; i++ {
		got, err := mdb.Master(ctx)
		if err != nil {
			t.Error(err)
		}

		mdb.nmu.RLock()
		if got != mdb.nodes["master"] {
			t.Errorf("mdb.selectMaster() = %v, want %v", got, "master")
		}
		mdb.nmu.RUnlock()
	}

	mocks["slave"].ExpectQuery(testMasterQuery).WillDelayFor(50 * time.Millisecond).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(false))
	mocks["borked"].ExpectQuery(testMasterQuery).WillDelayFor(time.Second).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(false))
	mocks["errored"].ExpectQuery(testMasterQuery).WillReturnError(sql.ErrConnDone)

	mdb.master.Load().(*sql.DB).Close()
	mdb.Delete("master")

	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	wantErrs := []error{
		&NoMasterErr{},
		sql.ErrConnDone,
	}

	got, err := mdb.SelectMaster(ctx)

	for _, w := range wantErrs {
		if !errors.As(err, &w) {
			t.Errorf("mdb.selectMaster() err = %T, want %T", err, w)
		}
	}

	if got != nil {
		t.Errorf("mdb.selectMaster() = %v, want %v", got, nil)
	}

	mdb = &MultiDB{}

	got, err = mdb.SelectMaster(ctx)
	if err == nil || err != ErrNoNodes {
		t.Errorf("electMaster() err = %v, want %v", err, ErrNoNodes)
	}
	if got != nil {
		t.Errorf("electMaster() = %v, want %v", got, nil)
	}
}

func TestMultiDB_MasterTx(t *testing.T) {
	db, mock, err := sm.New()
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectQuery(testMasterQuery).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(true))
	mock.ExpectBegin()

	tests := []struct {
		name    string
		mdb     *MultiDB
		ctx     context.Context
		wantErr bool
	}{
		{
			"No nodes",
			&MultiDB{},
			context.Background(),
			true,
		},
		{
			"Single node",
			&MultiDB{
				nodes: map[string]*sql.DB{
					"one": db,
				},
				MasterFunc: IsMaster(testMasterQuery),
			},
			context.Background(),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.wantErr {
				mock.ExpectBegin()
			}
			mdb := tt.mdb
			got, err := mdb.MasterTx(tt.ctx, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("MultiDB.Master() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == nil {
				t.Errorf("MultiDB.Master() = %v, want %v", got, "Tx")
			}
		})
	}
}

func TestMultiDB_Node(t *testing.T) {
	tests := []struct {
		name    string
		mdb     *MultiDB
		want    bool
		wantErr error
	}{
		{
			"No nodes",
			&MultiDB{},
			false,
			ErrNoNodes,
		},
		{
			"Single node",
			&MultiDB{
				nodes: map[string]*sql.DB{
					"one": new(sql.DB),
				},
			},
			true,
			nil,
		},
		{
			"Multiple nodes",
			&MultiDB{
				nodes: map[string]*sql.DB{
					"one":   new(sql.DB),
					"two":   new(sql.DB),
					"three": new(sql.DB),
				},
			},
			true,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mdb := tt.mdb
			got, err := mdb.Node()
			if fmt.Sprint(err) != fmt.Sprint(tt.wantErr) {
				t.Errorf("MultiDB.Node() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want && got == nil {
				t.Errorf("MultiDB.Node() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMultiDB_NodeTx(t *testing.T) {
	db, mock, err := sm.New()
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()

	tests := []struct {
		name    string
		mdb     *MultiDB
		want    bool
		wantErr error
	}{
		{
			"No nodes",
			&MultiDB{},
			false,
			ErrNoNodes,
		},
		{
			"Single node",
			&MultiDB{
				nodes: map[string]*sql.DB{
					"one": db,
				},
			},
			true,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mdb := tt.mdb
			got, err := mdb.NodeTx(context.Background(), nil)
			if fmt.Sprint(err) != fmt.Sprint(tt.wantErr) {
				t.Errorf("MultiDB.Node() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want && got == nil {
				t.Errorf("MultiDB.Node() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMultiDB_All(t *testing.T) {
	mdb := &MultiDB{
		nodes: map[string]*sql.DB{
			"one":   new(sql.DB),
			"two":   new(sql.DB),
			"three": new(sql.DB),
		},
	}

	nodes := mdb.All()
	if len(nodes) != len(mdb.nodes) {
		t.Errorf("mdb.All() got len %d, want len %d", len(nodes), len(mdb.nodes))
	}
}

func TestMultiDB_MultiNode(t *testing.T) {
	tests := []struct {
		name    string
		mdb     *MultiDB
		max     int
		want    int
		wantErr error
	}{
		{
			"ErrNoNodes",
			&MultiDB{},
			0,
			0,
			ErrNoNodes,
		},
		{
			"No limit",
			&MultiDB{
				nodes: map[string]*sql.DB{
					"one":   new(sql.DB),
					"two":   new(sql.DB),
					"three": new(sql.DB),
				},
			},
			0,
			3,
			nil,
		},
		{
			"Two nodes",
			&MultiDB{
				nodes: map[string]*sql.DB{
					"one":   new(sql.DB),
					"two":   new(sql.DB),
					"three": new(sql.DB),
				},
			},
			2,
			2,
			nil,
		},
		{
			"Too many",
			&MultiDB{
				nodes: map[string]*sql.DB{
					"one":   new(sql.DB),
					"two":   new(sql.DB),
					"three": new(sql.DB),
				},
			},
			4,
			3,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.mdb.MultiNode(tt.max, func(error) {})
			if fmt.Sprint(tt.wantErr) != fmt.Sprint(err) {
				t.Errorf("MultiDB.MultiNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr == nil {
				if len(got.nodes) != tt.want {
					t.Errorf("MultiDB.MultiNode() = %v, want %v", got, tt.want)
				}

				if got.errCallback == nil {
					t.Errorf("MultiDB.MultiNode() errCallback = %v, want %v", got, "func(error)")
				}
			}
		})
	}
}

func TestMultiDB_MultiNodeTx(t *testing.T) {
	mdb := new(MultiDB)

	for i := 0; i < 3; i++ {
		db, mock, err := sm.New()
		if err != nil {
			t.Fatal(err)
		}

		mock.ExpectBegin()

		mdb.Add(strconv.Itoa(i), db)
	}

	tests := []struct {
		name    string
		mdb     *MultiDB
		max     int
		want    int
		wantErr error
	}{
		{
			"ErrNoNodes",
			&MultiDB{},
			0,
			0,
			ErrNoNodes,
		},
		{
			"No limit",
			mdb,
			3,
			3,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.mdb.MultiTx(context.Background(), nil, tt.max, func(error) {})
			if fmt.Sprint(tt.wantErr) != fmt.Sprint(err) {
				t.Errorf("MultiDB.MultiTx() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr == nil {
				if len(got.tx) != tt.want {
					t.Errorf("MultiDB.MultiTx() = %v, want %v", got, tt.want)
				}

				if got.errCallback == nil {
					t.Errorf("MultiDB.MultiTx() errCallback = %#v, want %v", got, "func(error)")
				}
			}
		})
	}
}
