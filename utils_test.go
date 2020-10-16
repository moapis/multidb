// Copyright (c) 2020, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	sm "github.com/DATA-DOG/go-sqlmock"
	"github.com/moapis/multidb/drivers/postgresql"
)

const testMasterQuery = "select ismaster"

func TestIsMaster(t *testing.T) {
	db, mock, err := sm.New()
	if err != nil {
		t.Fatal(err)
	}

	mf := IsMaster(testMasterQuery)

	mock.ExpectQuery(testMasterQuery).
		WillReturnRows(sm.NewRows([]string{"ismaster"}).AddRow(true))

	b, err := mf(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	if !b {
		t.Errorf("MasterFunc = %v, want %v", b, true)
	}

	mock.ExpectQuery(testMasterQuery).
		WillReturnRows(sm.NewRows([]string{"ismaster"}).AddRow(false))

	b, err = mf(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	if b {
		t.Errorf("MasterFunc = %v, want %v", b, false)
	}

	mock.ExpectQuery(testMasterQuery).WillReturnError(sql.ErrNoRows)

	b, err = mf(context.Background(), db)
	if err != sql.ErrNoRows {
		t.Fatalf("MasterFunc err = %v, want %v", err, sql.ErrNoRows)
	}

	if b {
		t.Errorf("MasterFunc = %v, want %v", b, true)
	}
}

func TestMultiDB_AutoMasterSelector(t *testing.T) {
	tests := []struct {
		name      string
		whiteList []error
		want      []error
	}{
		{
			"Default whitelist",
			nil,
			append(DefaultWhiteListErrs, ErrNoNodes),
		},
		{
			"Passed whitelist",
			[]error{
				errTest,
			},
			[]error{
				errTest,
				ErrNoNodes,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mdb := new(MultiDB)

			got := mdb.AutoMasterSelector(time.Second, tt.whiteList...)
			if len(got.whiteList) != len(tt.want) {
				t.Errorf("MultiDB.AutoMasterSelector() = %v, want %v", got.whiteList, tt.want)
			}

			for i, err := range got.whiteList {
				if err != tt.want[i] {
					t.Errorf("MultiDB.AutoMasterSelector() = %v, want %v", got.whiteList, tt.want)
				}
			}

			if got.ctx == nil || got.cancel == nil {
				t.Errorf("MultiDB.AutoMasterSelector() ctx: %v or cancel: %v", got.ctx, got.cancel)
			}
		})
	}
}

func TestAutoMasterSelector_Close(t *testing.T) {
	ms := new(MultiDB).AutoMasterSelector(time.Second)

	ms.Close()

	ms.CheckErr(errTest)

	// Will deadlock if close did not do its job.
	<-ms.ctx.Done()
	<-ms.closed
}

func TestAutoMasterSelector_selectMaster(t *testing.T) {
	mdb := &MultiDB{
		MasterFunc: IsMaster(testMasterQuery),
	}

	db, _, err := sm.New()
	if err != nil {
		t.Fatal(err)
	}

	mdb.Add("one", db)

	ms := mdb.AutoMasterSelector(time.Minute)
	defer ms.Close()

	// Multiple execution runs different execution paths
	for i := 0; i < 100; i++ {
		ms.selectMaster()
	}
}

func TestMultiDB_MasterErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			"Nil",
			nil,
		},
		{
			"Closed",
			errTest,
		},
		{
			"Whitelist",
			sql.ErrNoRows,
		},
		{
			"No nodes", // Also whitelist
			ErrNoNodes,
		},
		{
			"Wrapped SelectMaster erorr",
			fmt.Errorf("spanac %w", &NoMasterErr{errTest}),
		},
		{
			"Connection error",
			sql.ErrConnDone,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sm.New()
			if err != nil {
				t.Fatal(err)
			}

			mdb := &MultiDB{
				MasterFunc: IsMaster(testMasterQuery),
			}
			mdb.Add("one", db)

			ms := mdb.AutoMasterSelector(time.Second)
			defer ms.Close()

			if tt.name == "Closed" {
				ms.Close()
			}

			mock.ExpectQuery(testMasterQuery).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(true))

			if err := ms.CheckErr(tt.err); err != tt.err {
				t.Errorf("MultiDB.MasterErr() error = %v, wantErr %v", err, tt.err)
			}
		})
	}
}

func ExampleAutoMasterSelector() {
	mdb := &MultiDB{
		MasterFunc: IsMaster(postgresql.MasterQuery),
	}

	ms := mdb.AutoMasterSelector(10 * time.Second)

	tx, err := mdb.MasterTx(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer ms.CheckErr(tx.Rollback())

	_, err = tx.Exec("insert something")
	if ms.CheckErr(err) != nil {
		// If error is not nil and not in whitelist,
		// mdb.SelectMaster will be run in a Go routine

		log.Println(err)
		return
	}

	err = ms.CheckErr(tx.Commit())
	if err != nil {
		log.Println(err)
		return
	}

}
