package multidb

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/volatiletech/sqlboiler/boil"
)

const (
	testDBDriver = "sqlite3"
	testDSN      = "file::memory:"
)

// Interface implementation checks
var (
	_ = boil.Executor(&Node{})
	_ = boil.ContextExecutor(&Node{})
)

func Test_newNodeStats(t *testing.T) {
	type args struct {
		statsLen    int
		failPercent int
	}
	tt := struct {
		name string
		args args
		want nodeStats
	}{
		"newNodeStats",
		args{
			statsLen:    1000,
			failPercent: 50,
		},
		nodeStats{
			failPercent: 50,
			fails:       make([]bool, 1000),
		},
	}

	t.Run(tt.name, func(t *testing.T) {
		got := newNodeStats(tt.args.statsLen, tt.args.failPercent)
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
	confs := []int{0, 50, 99, 100, -1}
	type args struct {
		state bool
	}
	tests := []struct {
		name  string
		args  args
		wants []bool
	}{
		{
			"0%",
			args{false},
			[]bool{false, false, false, false, false},
		},
		{
			"Up to 25%",
			args{true},
			[]bool{true, false, false, false, false},
		},
		{
			"Up to 50%",
			args{true},
			[]bool{true, false, false, false, false},
		},
		{
			"Up to 75%",
			args{true},
			[]bool{true, true, false, false, false},
		},
		{
			"Up to 100% and wrap",
			args{true},
			[]bool{true, true, true, false, false},
		},
		{
			"Down to 75%",
			args{false},
			[]bool{true, true, false, false, false},
		},
		{
			"Down to 50%",
			args{false},
			[]bool{true, false, false, false, false},
		},
	}
	for n, c := range confs {
		s := newNodeStats(4, c)
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
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
		driverName     string
		dataSourceName string
		statsLen       int
		failPercent    int
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
				driverName:     testDBDriver,
				dataSourceName: testDSN,
				statsLen:       1000,
				failPercent:    22,
				reconnectWait:  5 * time.Second,
			},
			&Node{
				nodeStats: nodeStats{
					failPercent: 22,
					fails:       make([]bool, 1000),
				},
				driverName:     testDBDriver,
				dataSourceName: testDSN,
				reconnectWait:  5 * time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newNode(tt.args.driverName, tt.args.dataSourceName, tt.args.statsLen, tt.args.failPercent, tt.args.reconnectWait); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNode_Open(t *testing.T) {
	type args struct {
		driverName, dataSourceName string
		statsLen, failPercent      int
		reconnectWait              time.Duration
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"Success",
			args{
				driverName:     testDBDriver,
				dataSourceName: testDSN,
				statsLen:       1000,
				failPercent:    22,
				reconnectWait:  5 * time.Second,
			},
			false,
		},
		{
			ErrAlreadyOpen,
			args{
				driverName:     testDBDriver,
				dataSourceName: testDSN,
				statsLen:       1000,
				failPercent:    22,
				reconnectWait:  5 * time.Second,
			},
			true,
		},
		{
			"Bogus",
			args{
				driverName:     "foo",
				dataSourceName: "bar",
				statsLen:       1000,
				failPercent:    22,
				reconnectWait:  5 * time.Second,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newNode(tt.args.driverName, tt.args.dataSourceName, tt.args.statsLen, tt.args.failPercent, tt.args.reconnectWait)
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
			newNode(testDBDriver, testDSN, 1000, 22, 0),
			false,
		},
		{
			"Closed",
			newNode(testDBDriver, testDSN, 1000, 22, 0),
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
	n := newNode(testDBDriver, testDSN, 1000, 22, 0)
	tests := []bool{true, true, false, true}
	for _, b := range tests {
		n.setReconnecting(b)
		if n.reconnecting != b {
			t.Errorf("Node.setReconnecting() got = %v, want %v", n.reconnecting, b)
		}
	}
}

func TestNode_reconnect(t *testing.T) {
	opened := newNode(testDBDriver, testDSN, 1000, 22, 5*time.Millisecond)
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
			newNode(testDBDriver, testDSN, 1000, 22, 5*time.Millisecond),
			true,
		},
		{
			"Already open",
			opened,
			true,
		},
		{
			"No reconnect",
			newNode(testDBDriver, testDSN, 1000, 22, 0),
			false,
		},
		{
			"Loop", // Will always fail
			newNode("boogie", "woogie", 1000, 22, 5*time.Millisecond),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "Loop" {
				// Break the loop after 1 sec.
				go func() {
					time.Sleep(time.Second)
					tt.node.reconnectWait = 0
				}()
			}
			tt.node.reconnect()
			if (tt.node.db != nil) != tt.wantDB {
				t.Errorf("Node.Open() DB = %v, wantDB %v", tt.node.db, tt.wantDB)
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

func TestNode_DB(t *testing.T) {
	// n.DB() should catch a <nil> node an prevent panic
	var n *Node
	db, err := n.DB()
	if err == nil {
		t.Errorf("Node.DB() Err = %v, want %v", err, sql.ErrConnDone)
	}

	n = newNode(testDBDriver, testDSN, 1000, 22, 0)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}
	db, err = n.DB()
	if err != nil {
		t.Error(err)
	}
	if db == nil {
		t.Errorf("Node.DB() DB = %v, want DB", db)
	}

	n.Close()
	db, err = n.DB()
	if err == nil {
		t.Errorf("Node.DB() Err = %v, want %v", err, sql.ErrConnDone)
	}
}

func TestNode_InUse(t *testing.T) {
	n := newNode(testDBDriver, testDSN, 1000, 22, 0)
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
	n := newNode(testDBDriver, testDSN, 1000, 22, 0)
	n.setErr(errors.New(ErrAlreadyOpen))
	err := n.ConnErr()
	if err == nil || err.Error() != ErrAlreadyOpen {
		t.Errorf("Node.ConnErr() Err = %v, want %v", err, ErrAlreadyOpen)
	}

	n.mtx.Lock()
	n.mtx.Unlock()
}

func TestNode_checkFailed(t *testing.T) {
	n := newNode(testDBDriver, testDSN, 10, 50, 0)
	if err := n.Open(); err != nil {
		t.Fatal(n)
	}
	for i := 0; i < 6; i++ {
		if n.db == nil {
			t.Errorf("Node.checkFailed() DB = %v, want %v", n.db, "DB")
		}
		n.checkFailed(true)
	}
	if n.db != nil {
		t.Errorf("Node.checkFailed() DB = %v, want %v", n.db, nil)
	}
}

func TestNode_CheckErr(t *testing.T) {
	n := newNode(testDBDriver, testDSN, 10, 10, 0)
	if err := n.Open(); err != nil {
		t.Fatal(n)
	}

	whiteErrs := []error{
		nil,
		sql.ErrNoRows,
		sql.ErrTxDone,
	}
	for _, w := range whiteErrs {
		err := n.CheckErr(w)
		time.Sleep(time.Millisecond)
		if err != w {
			t.Errorf("Node.CheckErr() Err = %v, want %v", err, w)
		}
		if n.connErr != nil {
			t.Errorf("Node.CheckErr() connErr = %v, want %v", n.connErr, nil)
		}
		if n.db == nil {
			t.Errorf("Node.CheckErr() DB = %v, want %v", n.db, "DB")
		}
	}
	err := n.CheckErr(sql.ErrConnDone)
	time.Sleep(time.Millisecond)
	if err != sql.ErrConnDone {
		t.Errorf("Node.CheckErr() Err = %v, want %v", err, sql.ErrConnDone)
	}
	if n.connErr != sql.ErrConnDone {
		t.Errorf("Node.CheckErr() connErr = %v, want %v", n.connErr, sql.ErrConnDone)
	}
	if n.db == nil {
		t.Errorf("Node.CheckErr() DB = %v, want %v", n.db, "DB")
	}

	err = n.CheckErr(sql.ErrConnDone)
	time.Sleep(time.Millisecond)
	if err != sql.ErrConnDone {
		t.Errorf("Node.CheckErr() Err = %v, want %v", err, sql.ErrConnDone)
	}
	if n.connErr != sql.ErrConnDone {
		t.Errorf("Node.CheckErr() connErr = %v, want %v", n.connErr, sql.ErrConnDone)
	}
	if n.db != nil {
		t.Errorf("Node.CheckErr() DB = %v, want %v", n.db, nil)
	}
}

func TestNode_Exec(t *testing.T) {
	n := newNode(testDBDriver, testDSN, 10, 0, 0)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}
	r, err := n.Exec("select $1;", 1)
	if err != nil {
		t.Error(err)
	}
	if r == nil {
		t.Errorf("Node.Exec() R = %v, want %v", r, "Result")
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = n.Exec("select $1;", 1)
	if err != sql.ErrConnDone {
		t.Errorf("Node.Exec() connErr = %v, want %v", err, sql.ErrConnDone)
	}
}

func TestNode_Query(t *testing.T) {
	n := newNode(testDBDriver, testDSN, 10, 0, 0)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}
	r, err := n.Query("select $1;", 1)
	if err != nil {
		t.Error(err)
	}
	if r == nil {
		t.Errorf("Node.Query() R = %v, want %v", r, "Result")
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = n.Query("select $1;", 1)
	if err != sql.ErrConnDone {
		t.Errorf("Node.Query() connErr = %v, want %v", err, sql.ErrConnDone)
	}
}

func TestNode_QueryRow(t *testing.T) {
	n := newNode(testDBDriver, testDSN, 10, 0, 0)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}
	r := n.QueryRow("select $1;", 1)
	if r == nil {
		t.Errorf("Node.QueryRow() R = %v, want %v", r, "Result")
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	r = n.QueryRow("select $1;", 1)
	if r != nil {
		t.Errorf("Node.QueryRow() R = %v, want %v", r, nil)
	}
}

func TestNode_ExecContext(t *testing.T) {
	n := newNode(testDBDriver, testDSN, 10, 0, 0)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}
	r, err := n.ExecContext(context.Background(), "select $1;", 1)
	if err != nil {
		t.Error(err)
	}
	if r == nil {
		t.Errorf("Node.ExecContext() R = %v, want %v", r, "Result")
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = n.ExecContext(context.Background(), "select $1;", 1)
	if err != sql.ErrConnDone {
		t.Errorf("Node.ExecContext() connErr = %v, want %v", err, sql.ErrConnDone)
	}
}

func TestNode_QueryContext(t *testing.T) {
	n := newNode(testDBDriver, testDSN, 10, 0, 0)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}
	r, err := n.QueryContext(context.Background(), "select $1;", 1)
	if err != nil {
		t.Error(err)
	}
	if r == nil {
		t.Errorf("Node.QueryContext() R = %v, want %v", r, "Result")
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = n.QueryContext(context.Background(), "select $1;", 1)
	if err != sql.ErrConnDone {
		t.Errorf("Node.QueryContext() connErr = %v, want %v", err, sql.ErrConnDone)
	}
}

func TestNode_QueryRowContext(t *testing.T) {
	n := newNode(testDBDriver, testDSN, 10, 0, 0)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}
	r := n.QueryRowContext(context.Background(), "select $1;", 1)
	if r == nil {
		t.Errorf("Node.QueryRowContext() R = %v, want %v", r, "Result")
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	r = n.QueryRowContext(context.Background(), "select $1;", 1)
	if r != nil {
		t.Errorf("Node.QueryRowContext() R = %v, want %v", r, nil)
	}
}

func TestNode_Begin(t *testing.T) {
	n := newNode(testDBDriver, testDSN, 10, 0, 0)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}
	tx, err := n.Begin()
	if err != nil {
		t.Error(err)
	}
	if tx == nil {
		t.Errorf("Node.Begin() R = %v, want %v", tx, "TX")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	tx, err = n.Begin()
	if err != sql.ErrConnDone {
		t.Errorf("Node.Begin() connErr = %v, want %v", err, sql.ErrConnDone)
	}
}

func TestNode_BeginTx(t *testing.T) {
	n := newNode(testDBDriver, testDSN, 10, 0, 0)
	if err := n.Open(); err != nil {
		t.Fatal(err)
	}
	tx, err := n.BeginTx(context.Background(), nil)
	if err != nil {
		t.Error(err)
	}
	if tx == nil {
		t.Errorf("Node.BeginTx() R = %v, want %v", tx, "TX")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	tx, err = n.BeginTx(context.Background(), nil)
	if err != sql.ErrConnDone {
		t.Errorf("Node.BeginTx() connErr = %v, want %v", err, sql.ErrConnDone)
	}
}
