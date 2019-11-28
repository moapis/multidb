// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"sync"
	"time"
)

const (
	// ErrAlreadyOpen is returned when Opening on an already open Node
	ErrAlreadyOpen = "Node already open"
)

type nodeStats struct {
	maxFails int
	fails    []bool
	pos      int
	mtx      sync.Mutex
}

func newNodeStats(statsLen, maxFails int) nodeStats {
	return nodeStats{
		maxFails: maxFails,
		fails:    make([]bool, statsLen),
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
	if s.maxFails < 0 || len(s.fails) == 0 {
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
	return count > s.maxFails
}

// Node represents a database server connection
type Node struct {
	nodeStats
	driverName     string
	dataSourceName string
	db             *sql.DB
	connErr        error
	reconnectWait  time.Duration
	reconnecting   bool
	mtx            sync.RWMutex
}

func newNode(driverName, dataSourceName string, statsLen, maxFails int, reconnectWait time.Duration) *Node {
	return &Node{
		nodeStats:      newNodeStats(statsLen, maxFails),
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

func (n *Node) setReconnecting(s bool) {
	n.mtx.Lock()
	n.reconnecting = s
	n.mtx.Unlock()
}

func (n *Node) reconnect(ctx context.Context) {
	if n.reconnectWait == 0 {
		return
	}

	for ctx.Err() == nil {
		n.setReconnecting(true)
		defer n.setReconnecting(false)

		time.Sleep(n.reconnectWait)
		if err := n.Open(); err == nil || err.Error() == ErrAlreadyOpen {
			return
		}
	}
}

// Reconnecting returns true while reconnecting is in progress
func (n *Node) Reconnecting() bool {
	n.mtx.RLock()
	defer n.mtx.RUnlock()

	return n.reconnecting
}

// InUse get the InUse counter from db.Stats.
// Returns -1 in case db is unavailable.
func (n *Node) InUse() int {
	n.mtx.RLock()
	defer n.mtx.RUnlock()

	if n.db == nil {
		return -1
	}
	return n.db.Stats().InUse
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
		n.reconnect(context.Background())
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

// ExecContext wrapper around sql.DB.Exec.
// Implements boil.ContextExecutor
func (n *Node) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	n.mtx.RLock()
	res, err := n.db.ExecContext(ctx, query, args...)
	n.mtx.RUnlock()

	return res, n.CheckErr(err)
}

// Exec wrapper around sql.DB.Exec.
// Implements boil.Executor
func (n *Node) Exec(query string, args ...interface{}) (sql.Result, error) {
	return n.ExecContext(context.Background(), query, args...)
}

// QueryContext wrapper around sql.DB.Query.
// Implements boil.ContextExecutor
func (n *Node) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	n.mtx.RLock()
	rows, err := n.db.QueryContext(ctx, query, args...)
	n.mtx.RUnlock()

	return rows, n.CheckErr(err)
}

// Query wrapper around sql.DB.Query.
// Implements boil.Executor
func (n *Node) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return n.QueryContext(context.Background(), query, args...)
}

// QueryRowContext wrapper around sql.DB.QueryRow.
// Implements boil.ContextExecutor
// Since errors are defered untill row.Scan, this package cannot monitor such errors.
func (n *Node) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	n.mtx.RLock()
	row := n.db.QueryRowContext(ctx, query, args...)
	n.mtx.RUnlock()

	return row
}

// QueryRow wrapper around sql.DB.QueryRow.
// Implements boil.Executor
// Since errors are defered untill row.Scan, this package cannot monitor such errors.
func (n *Node) QueryRow(query string, args ...interface{}) *sql.Row {
	return n.QueryRowContext(context.Background(), query, args...)
}

// BeginTx opens a new *sql.Tx inside a Tx.
// Does NOT implement boil.Beginner, as it requires a *sql.Tx.
func (n *Node) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	n.mtx.RLock()
	tx, err := n.db.BeginTx(ctx, opts)
	n.mtx.RUnlock()

	return &Tx{n, tx}, n.CheckErr(err)
}

// Begin opens a new *sql.Tx inside a Tx.
// Does NOT implement boil.Beginner, as it requires a *sql.Tx.
func (n *Node) Begin() (*Tx, error) {
	return n.BeginTx(context.Background(), nil)
}

type entry struct {
	node   *Node
	factor float32
}

// nodeList implements sort.Interface
type entries []entry

func (ent entries) Len() int           { return len(ent) }
func (ent entries) Less(i, j int) bool { return ent[i].factor < ent[j].factor }
func (ent entries) Swap(i, j int)      { ent[i], ent[j] = ent[j], ent[i] }

func newEntries(nodes []*Node) entries {
	var ent entries
	for _, n := range nodes {
		if n != nil && n.db != nil {
			st := n.db.Stats()
			ent = append(ent, entry{
				node:   n,
				factor: float32(st.InUse) / float32(st.MaxOpenConnections),
			})
		}
	}
	return ent
}

// sortAndSlice sorts the entries and return up to `no` amount of Nodes.
func (ent entries) sortAndSlice(max int) []*Node {
	sort.Sort(ent)

	if len(ent) < max {
		max = len(ent)
	}

	nodes := make([]*Node, max)
	for i := 0; i < max; i++ {
		nodes[i] = ent[i].node
	}
	return nodes
}

// availableNodes returns a slice of available *Node.
// The slice is sorted by the division of InUse/MaxOpenConnections.
// Up to `no` amount of nodes is in the returned slice.
// Nil is returned in case no nodes are available.
func availableNodes(nodes []*Node, max int) ([]*Node, error) {
	for _, n := range nodes {
		n.mtx.RLock()
		defer n.mtx.RUnlock()
	}

	ent := newEntries(nodes)
	if ent == nil {
		return nil, errors.New(ErrNoNodes)
	}
	return ent.sortAndSlice(max), nil
}
