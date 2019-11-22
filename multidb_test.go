package multidb

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/moapis/multidb/drivers"
)

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

func TestConfig_Open(t *testing.T) {
	type fields struct {
		DBConf        drivers.Configurator
		StatsLen      int
		FailPrecent   int
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
				FailPrecent:   20,
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
				FailPrecent:   20,
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
				FailPrecent:   20,
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
				FailPrecent:   20,
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
				FailPrecent:   tt.fields.FailPrecent,
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
				if (n.driverName == "nil") != (n.db == nil) {
					t.Errorf("Config.Open() = %v, want %v", n.db, n.driverName)
				}
				if n.driverName == "nil" && !n.Reconnecting() {
					t.Errorf("Config.Open() Reconnecting = %v, want %v", n.Reconnecting(), true)
				}
			}
		})
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
			// This will fal untill we implement multi node
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
