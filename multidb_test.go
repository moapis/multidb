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
	"strconv"
	"testing"
	"time"

	sm "github.com/DATA-DOG/go-sqlmock"
	"github.com/moapis/multidb/drivers"
)

const (
	testDBDriver = "sqlmock"
	testDSN      = "file::memory:"
	testQuery    = "select;"
)

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

func TestMultiDB_Close(t *testing.T) {
	mdb := &MultiDB{}

	db, mock, err := sm.New()
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectClose()

	mdb.Add(NewNode("ok", db))

	db, mock, err = sm.New()
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectClose().WillReturnError(sql.ErrConnDone)

	mdb.Add(NewNode("err", db))

	err = mdb.Close()
	if err != sql.ErrConnDone {
		t.Errorf("mdb.Close() err = %v, want %v", err, sql.ErrConnDone)
	}
}

func TestMultiDB_Add_Delete(t *testing.T) {
	nodes := []*Node{
		{"one", &sql.DB{}},
		{"two", &sql.DB{}},
		{"three", &sql.DB{}},
	}

	mdb := new(MultiDB)

	mdb.Add(nodes...)
	mdb.Delete("one", "two")
}

const testMasterQuery = "select ismaster"

// Includes test for mdb.Master()
func TestMultiDB_selectMaster(t *testing.T) {
	mocks := map[string]sm.Sqlmock{"master": nil, "slave": nil, "borked": nil, "errored": nil}

	mdb := &MultiDB{
		MasterFunc: drivers.MasterFunc(testMasterQuery),
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

		mdb.Add(NewNode(k, db))
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
		if got.name != "master" {
			t.Errorf("mdb.selectMaster() = %v, want %v", got, "master")
		}
	}

	delete(mocks, "master")
	mocks["slave"].ExpectQuery(testMasterQuery).WillDelayFor(50 * time.Millisecond).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(false))
	mocks["borked"].ExpectQuery(testMasterQuery).WillDelayFor(time.Second).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(false))
	mocks["errored"].ExpectQuery(testMasterQuery).WillReturnError(sql.ErrConnDone)

	mdb.master.Load().(*Node).Close()

	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	got, err := mdb.SelectMaster(ctx)
	_, ok := err.(*MultiError)
	if !ok {
		t.Errorf("mdb.selectMaster() err = %T, want %T", err, MultiError{})
	}
	if got != nil {
		t.Errorf("mdb.selectMaster() = %v, want %v", got, nil)
	}

	mdb = &MultiDB{}

	got, err = mdb.SelectMaster(ctx)
	if err == nil || err.Error() != ErrNoNodes {
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
				nm: nodeMap{
					nodes: map[string]*Node{
						"one": {"one", db},
					},
				},
				MasterFunc: drivers.MasterFunc(testMasterQuery),
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
			errors.New(ErrNoNodes),
		},
		{
			"Single node",
			&MultiDB{
				nm: nodeMap{
					nodes: map[string]*Node{
						"one": {"one", &sql.DB{}},
					},
				},
			},
			true,
			nil,
		},
		{
			"Multiple nodes",
			&MultiDB{
				nm: nodeMap{
					nodes: map[string]*Node{
						"one":   {"one", &sql.DB{}},
						"two":   {"two", &sql.DB{}},
						"three": {"three", &sql.DB{}},
					},
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
			errors.New(ErrNoNodes),
		},
		{
			"Single node",
			&MultiDB{
				nm: nodeMap{
					nodes: map[string]*Node{
						"one": {"one", db},
					},
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
		nm: nodeMap{
			nodes: map[string]*Node{
				"one":   {"one", &sql.DB{}},
				"two":   {"two", &sql.DB{}},
				"three": {"three", &sql.DB{}},
			},
		},
	}

	nodes := mdb.All()
	if len(nodes) != len(mdb.nm.nodes) {
		t.Errorf("mdb.All() got len %d, want len %d", len(nodes), len(mdb.nm.nodes))
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
			errors.New(ErrNoNodes),
		},
		{
			"No limit",
			&MultiDB{
				nm: nodeMap{
					nodes: map[string]*Node{
						"one":   {"one", &sql.DB{}},
						"two":   {"two", &sql.DB{}},
						"three": {"three", &sql.DB{}},
					},
				},
			},
			0,
			3,
			nil,
		},
		{
			"Two nodes",
			&MultiDB{
				nm: nodeMap{
					nodes: map[string]*Node{
						"one":   {"one", &sql.DB{}},
						"two":   {"two", &sql.DB{}},
						"three": {"three", &sql.DB{}},
					},
				},
			},
			2,
			2,
			nil,
		},
		{
			"Too many",
			&MultiDB{
				nm: nodeMap{
					nodes: map[string]*Node{
						"one":   {"one", &sql.DB{}},
						"two":   {"two", &sql.DB{}},
						"three": {"three", &sql.DB{}},
					},
				},
			},
			4,
			3,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.mdb.MultiNode(tt.max)
			if fmt.Sprint(tt.wantErr) != fmt.Sprint(err) {
				t.Errorf("MultiDB.MultiNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != tt.want {
				t.Errorf("MultiDB.MultiNode() = %v, want %v", got, tt.want)
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

		mdb.Add(NewNode(strconv.Itoa(i), db))
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
			errors.New(ErrNoNodes),
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
			got, err := tt.mdb.MultiTx(context.Background(), nil, tt.max)
			if fmt.Sprint(tt.wantErr) != fmt.Sprint(err) {
				t.Errorf("MultiDB.MultiTx() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.want > 0 && len(got.tx) != tt.want {
				t.Errorf("MultiDB.MultiTx() = %v, want %v", got, tt.want)
			}
		})
	}
}
