// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	sm "github.com/DATA-DOG/go-sqlmock"
	"github.com/moapis/multidb/drivers"
	"github.com/moapis/multidb/drivers/postgresql"
)

func TestConfig_Open(t *testing.T) {
	type fields struct {
		DBConf        drivers.Configurator
		StatsLen      int
		MaxFails      int
		ReconnectWait time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"No nodes",
			fields{
				DBConf: testConfig{
					dn: testDBDriver,
				},
				StatsLen:      100,
				MaxFails:      20,
				ReconnectWait: 0,
			},
			true,
		},
		{
			"One node",
			fields{
				DBConf: testConfig{
					dn:   testDBDriver,
					dsns: []string{testDSN},
				},
				StatsLen:      100,
				MaxFails:      20,
				ReconnectWait: 0,
			},
			false,
		},
		{
			"Multi node",
			fields{
				DBConf: testConfig{
					dn:   testDBDriver,
					dsns: []string{testDSN, testDSN, testDSN, testDSN},
				},
				StatsLen:      100,
				MaxFails:      20,
				ReconnectWait: 0,
			},
			false,
		},
		{
			"Connection failures",
			fields{
				DBConf: testConfig{
					dn:   "nil",
					dsns: []string{testDSN, testDSN, testDSN, testDSN},
				},
				StatsLen:      100,
				MaxFails:      20,
				ReconnectWait: time.Minute,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				DBConf:        tt.fields.DBConf,
				StatsLen:      tt.fields.StatsLen,
				MaxFails:      tt.fields.MaxFails,
				ReconnectWait: tt.fields.ReconnectWait,
			}
			got, err := c.Open()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(c.DBConf.DataSourceNames()) == 0 {
				return
			}
			if len(got.all) != len(c.DBConf.DataSourceNames()) {
				t.Errorf("Config.Open() Nodes # = %v, want %v", len(got.all), len(c.DBConf.DataSourceNames()))
			}
			time.Sleep(time.Millisecond) // Give some time for the recovery go routine to kick in
			for _, n := range got.all {
				if (n.DriverName() == "nil") != (n.db == nil) {
					t.Errorf("Config.Open() = %v, want %v", n.db, n.DriverName())
				}
				if n.DriverName() == "nil" && !n.Reconnecting() {
					t.Errorf("Config.Open() Reconnecting = %v, want %v", n.Reconnecting(), true)
				}
			}
		})
	}
}

func Example() {
	c := Config{
		DBConf: postgresql.Config{
			Hosts: []postgresql.Host{
				{
					Addr: "db1.example.com",
					Port: 5432,
				},
				{
					Addr: "db2.example.com",
					Port: 5432,
				},
				{
					Addr: "db3.example.com",
					Port: 5432,
				},
			},
			Params: map[string]string{
				"dbname":          "multidb",
				"user":            "postgres",
				"password":        "",
				"sslmode":         "disable",
				"connect_timeout": "30",
			},
		},
		StatsLen:      100,
		MaxFails:      10,
		ReconnectWait: 10 * time.Second,
	}
	// Connect to all specified DB Hosts
	mdb, err := c.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer mdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Open a TX for insertion on the master.
	// Master assertion is done in the background on first access.
	tx, err := mdb.MasterTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	// Do stuff inside the transaction
	if _, err = tx.ExecContext(ctx, "CREATE TABLE content ( id INTEGER PRIMARY KEY );"); err != nil {
		log.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO content (id) VALUES ($1);", 999); err != nil {
		log.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO content (id) VALUES ($1);", 101); err != nil {
		log.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// Acquire 3 nodes for select operation
	mn, err := mdb.MultiNode(3)
	if err != nil {
		log.Fatal(err)
	}
	rows, err := mn.QueryContext(ctx, "SELECT id FROM content WHERE id = $1", 999)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var i int
		if err = rows.Scan(&i); err != nil {
			log.Fatal(err)
		}
		fmt.Println(i)
	}

	// Acquire the master node without Tx
	master, err := mdb.Master(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Exec without context
	if _, err = master.Exec("DROP TABLE content"); err != nil {
		log.Fatal(err)
	}
}

var (
	testSingleConf = Config{
		DBConf: testConfig{
			dn:   testDBDriver,
			dsns: []string{testDSN},
		},
	}
	testMultiConf = Config{
		DBConf: testConfig{
			dn:   testDBDriver,
			dsns: []string{testDSN, testDSN, testDSN, testDSN},
		},
	}
)

func Test_electMaster(t *testing.T) {
	mocks := map[string]sm.Sqlmock{"master": nil, "slave": nil, "borked": nil, "errored": nil}
	var (
		nodes []*Node
		exp   *Node
	)
	for k := range mocks {
		db, mock, err := sm.New()
		if err != nil {
			t.Fatal(err)
		}
		mocks[k] = mock
		node := &Node{
			Configurator: defaultTestConfig(),
			db:           db,
			nodeStats: nodeStats{
				maxFails: -1,
			},
		}
		if k == "master" {
			exp = node
		}
		nodes = append(nodes, node)
	}
	nodes = append(nodes, nil)

	q := defaultTestConfig().MasterQuery()
	mocks["master"].ExpectQuery(q).WillDelayFor(100 * time.Millisecond).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(true))
	mocks["slave"].ExpectQuery(q).WillDelayFor(50 * time.Millisecond).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(false))
	mocks["borked"].ExpectQuery(q).WillDelayFor(time.Second)
	mocks["errored"].ExpectQuery(q).WillReturnError(sql.ErrConnDone)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	got, err := electMaster(ctx, nodes)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("electMaster() = %v, want %v", got, exp)
	}

	delete(mocks, "master")
	mocks["slave"].ExpectQuery(q).WillDelayFor(50 * time.Millisecond).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(false))
	mocks["borked"].ExpectQuery(q).WillDelayFor(time.Second).WillReturnRows(sm.NewRows([]string{"master"}).AddRow(false))
	mocks["errored"].ExpectQuery(q).WillReturnError(sql.ErrConnDone)
	exp.Close()
	exp = nil

	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	got, err = electMaster(ctx, nodes)
	_, ok := err.(MultiError)
	if !ok {
		t.Errorf("electMaster() err = %T, want %T", err, MultiError{})
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("electMaster() = %v, want %v", got, exp)
	}

	got, err = electMaster(ctx, nil)
	if err == nil || err.Error() != ErrNoNodes {
		t.Errorf("electMaster() err = %v, want %v", err, ErrNoNodes)
	}
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("electMaster() = %v, want %v", got, exp)
	}
}

func TestMultiDB_setMaster(t *testing.T) {
	singleMDB, err := testSingleConf.Open()
	if err != nil {
		t.Fatal(err)
	}
	multiMDB, err := testMultiConf.Open()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		mdb     *MultiDB
		ctx     context.Context
		want    *Node
		wantErr bool
	}{
		{
			"No nodes",
			&MultiDB{},
			context.Background(),
			nil,
			true,
		},
		{
			"Single node",
			singleMDB,
			context.Background(),
			singleMDB.all[0],
			false,
		},
		{
			"Multi node",
			multiMDB,
			context.Background(),
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mdb := tt.mdb
			got, err := mdb.setMaster(tt.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("MultiDB.setMaster() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MultiDB.setMaster() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMultiDB_Master(t *testing.T) {
	singleMDB, err := testSingleConf.Open()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		mdb     *MultiDB
		ctx     context.Context
		want    *Node
		wantErr bool
	}{
		{
			"No nodes",
			&MultiDB{},
			context.Background(),
			nil,
			true,
		},
		{
			"Single node",
			singleMDB,
			context.Background(),
			singleMDB.all[0],
			false,
		},
		{
			"Subsequent single node",
			singleMDB,
			context.Background(),
			singleMDB.all[0],
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mdb := tt.mdb
			got, err := mdb.Master(tt.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("MultiDB.Master() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MultiDB.Master() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMultiDB_MasterTx(t *testing.T) {
	singleMDB, err := testSingleConf.Open()
	if err != nil {
		t.Fatal(err)
	}
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
			singleMDB,
			context.Background(),
			false,
		},
		{
			"Subsequent single node",
			singleMDB,
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
	singleMDB, err := testSingleConf.Open()
	if err != nil {
		t.Fatal(err)
	}
	multiMDB, err := testMultiConf.Open()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		mdb     *MultiDB
		want    *Node
		wantErr bool
	}{
		{
			"No nodes",
			&MultiDB{},
			nil,
			true,
		},
		{
			"Single node",
			singleMDB,
			singleMDB.all[0],
			false,
		},
		{
			"Multi node",
			multiMDB,
			multiMDB.all[0],
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mdb := tt.mdb
			got, err := mdb.Node()
			if (err != nil) != tt.wantErr {
				t.Errorf("MultiDB.Node() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MultiDB.Node() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMultiDB_NodeTx(t *testing.T) {
	singleMDB, err := testSingleConf.Open()
	if err != nil {
		t.Fatal(err)
	}
	multiMDB, err := testMultiConf.Open()
	if err != nil {
		t.Fatal(err)
	}

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
			singleMDB,
			context.Background(),
			false,
		},
		{
			"Multi node",
			multiMDB,
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
			got, err := mdb.NodeTx(tt.ctx, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("MultiDB.Node() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == nil {
				t.Errorf("MultiDB.Master() = %v, want %v", got, "Tx")
			}
		})
	}
}

func TestMultiDB_All(t *testing.T) {
	singleMDB, err := testSingleConf.Open()
	if err != nil {
		t.Fatal(err)
	}
	multiMDB, err := testMultiConf.Open()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		mdb  *MultiDB
		want []*Node
	}{
		{
			"No nodes",
			&MultiDB{},
			nil,
		},
		{
			"Single node",
			singleMDB,
			singleMDB.all,
		},
		{
			"Multi node",
			multiMDB,
			multiMDB.all,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mdb := tt.mdb
			if got := mdb.All(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MultiDB.All() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMultiDB_Close(t *testing.T) {
	mdb, err := testMultiConf.Open()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("mdb.Close()")
	if err := mdb.Close(); err != nil {
		t.Error(err)
	}
}

func TestMultiDB_MultiNode(t *testing.T) {
	singleMDB, err := testSingleConf.Open()
	if err != nil {
		t.Fatal(err)
	}
	multiMDB, err := testMultiConf.Open()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		mdb     *MultiDB
		want    MultiNode
		wantErr bool
	}{
		{
			"No nodes",
			&MultiDB{},
			nil,
			true,
		},
		{
			"Single node",
			singleMDB,
			singleMDB.all,
			false,
		},
		{
			"Multi node",
			multiMDB,
			multiMDB.all,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mdb := tt.mdb
			got, err := mdb.MultiNode(10)
			if (err != nil) != tt.wantErr {
				t.Errorf("MultiDB.MultiNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MultiDB.MultiNodev() = %v, want %v", got, tt.want)
			}
		})
	}
}
