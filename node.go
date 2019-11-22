package multidb

import (
	"context"
	"database/sql"
	"errors"
	"github.com/volatiletech/sqlboiler/boil"
	"sync"
	"time"
)

const (
	// ErrAlreadyOpen is returned when Opening on an already open Node
	ErrAlreadyOpen = "Node already open"
)

type nodeStats struct {
	failPercent int
	fails       []bool
	pos         int
	mtx         sync.Mutex
}

func newNodeStats(statsLen, failPercent int) nodeStats {
	return nodeStats{
		failPercent: failPercent,
		fails:       make([]bool, statsLen),
	}
}

// reset the success stats, needed on reconnect
// total usage remains intact
func (s *nodeStats) reset() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for i := 0; i < len(s.fails); i++ {
		s.fails[i] = false
	}
}

// failed counts a failure and calculates if the node is failed
func (s *nodeStats) failed(state bool) bool {
	if s.failPercent < 0 {
		return false
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.fails[s.pos] = state
	var count int
	for _, b := range s.fails {
		if b {
			count++
		}
	}
	if s.pos++; s.pos >= len(s.fails) {
		s.pos = 0
	}
	return count*100/len(s.fails) > s.failPercent
}

// Node represents a database server connection
type Node struct {
	nodeStats
	driverName     string
	dataSourceName string
	db             *sql.DB
	connErr        error
	reconnectWait  time.Duration
	mtx            sync.RWMutex
}

func newNode(driverName, dataSourceName string, statsLen, failPercent int, reconnectWait time.Duration) *Node {
	return &Node{
		nodeStats:      newNodeStats(statsLen, failPercent),
		driverName:     driverName,
		dataSourceName: dataSourceName,
		reconnectWait:  reconnectWait,
	}
}

// Open calls sql.Open() with the configured driverName and dataSourceName.
// Open should only be used after the Node was (auto-)closed and reconnection is disabled.
func (n *Node) Open() error {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	if n.db != nil {
		return errors.New(ErrAlreadyOpen)
	}

	n.db, n.connErr = sql.Open(n.driverName, n.dataSourceName)
	n.reset()

	return n.connErr
}

// Close the current node and make it unavailable
func (n *Node) Close() error {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	var err error
	if n.db == nil {
		err = sql.ErrConnDone
	} else {
		err = n.db.Close()
	}
	if err != nil {
		n.connErr = err
	}
	n.db = nil
	return err
}

func (n *Node) reconnect() {
	for n.reconnectWait != 0 { // 0 means no reconnecting
		time.Sleep(n.reconnectWait)
		if err := n.Open(); err == nil || err.Error() == ErrAlreadyOpen {
			return
		}
	}
}

// DB returns the raw sql.DB connection object
func (n *Node) DB() (*sql.DB, error) {
	n.mtx.RLock()
	defer n.mtx.RUnlock()

	if n.db == nil {
		return nil, sql.ErrConnDone
	}

	return n.db, nil
}

// InUse get the InUse counter from db.Stats.
// Returns -1 in case db is unavailable.
func (n *Node) InUse() int {
	db, err := n.DB()
	if err != nil {
		return -1
	}
	return db.Stats().InUse
}

func (n *Node) setErr(err error) {
	n.mtx.Lock()
	n.connErr = err
	n.mtx.Unlock()
}

// ConnErr returns the last encountered connection error for Node
func (n *Node) ConnErr() error {
	n.mtx.RLock()
	defer n.mtx.RUnlock()

	return n.connErr
}

// checkFailed closes this Node's DB pool if failed.
// Afer closing the reconnector is initiated, if applicable.
func (n *Node) checkFailed(state bool) {
	if n.failed(state) {
		n.Close()
		n.reconnect()
	}
}

// CheckErr updates the statistcs. If the error is nil or whitelisted, success is recorded.
// Any other case constitutes an error and failure is recorded.
// If a the configured failure trashhold is reached, this node will we disconnected.
//
// This method is already called by each database call method and need to be used in most cases.
// It is exported for use in extenting libraries which need use struct embedding
// and want to overload Node methods, while still keeping statistics up-to-date.
func (n *Node) CheckErr(err error) error {
	switch {
	case err == nil:
		go n.checkFailed(false)
	case err == sql.ErrNoRows || err == sql.ErrTxDone:
		go n.checkFailed(false)
	//case whiteList(err):
	//	go n.checkFailed(true)
	default:
		n.setErr(err)
		go n.checkFailed(true)
	}
	return err
}

// Exec wrapper around sql.DB.Exec.
// Implements boil.Executor
func (n *Node) Exec(query string, args ...interface{}) (sql.Result, error) {
	db, err := n.DB()
	if err != nil {
		return nil, err
	}
	res, err := db.Exec(query, args...)
	return res, n.CheckErr(err)
}

// Query wrapper around sql.DB.Query.
// Implements boil.Executor
func (n *Node) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db, err := n.DB()
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(query, args...)
	return rows, n.CheckErr(err)
}

// QueryRow wrapper around sql.DB.QueryRow.
// Implements boil.Executor
// Since errors are defered untill row.Scan, this package cannot monitor such errors.
func (n *Node) QueryRow(query string, args ...interface{}) *sql.Row {
	db, err := n.DB()
	if err != nil {
		return nil
	}
	return db.QueryRow(query, args...)
}

// ExecContext wrapper around sql.DB.Exec.
// Implements boil.ContextExecutor
func (n *Node) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	db, err := n.DB()
	if err != nil {
		return nil, err
	}
	res, err := db.ExecContext(ctx, query, args...)
	return res, n.CheckErr(err)
}

// QueryContext wrapper around sql.DB.Query.
// Implements boil.ContextExecutor
func (n *Node) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	db, err := n.DB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, query, args...)
	return rows, n.CheckErr(err)
}

// QueryRowContext wrapper around sql.DB.QueryRow.
// Implements boil.ContextExecutor
// Since errors are defered untill row.Scan, this package cannot monitor such errors.
func (n *Node) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	db, err := n.DB()
	if err != nil {
		return nil
	}
	return db.QueryRowContext(ctx, query, args...)
}

// Begin wrapper around sql.DB.Begin.
// Implements boil.Beginner
// Subsequent errors inside the transaction cannot be monitored by this package
func (n *Node) Begin() (boil.Transactor, error) {
	db, err := n.DB()
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	return tx, n.CheckErr(err)
}

// BeginTx wrapper around sql.DB.BeginTx.
// Implements boil.ContextBeginner
// Subsequent errors inside the transaction cannot be monitored by this package
func (n *Node) BeginTx(ctx context.Context, opts *sql.TxOptions) (boil.Transactor, error) {
	db, err := n.DB()
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, opts)
	return tx, n.CheckErr(err)
}
