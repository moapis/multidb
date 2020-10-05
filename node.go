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

	"github.com/moapis/multidb/drivers"
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

// Node represents a database server connection.
// Node Implements boil.ContextExecutor
type Node struct {
	nodeStats
	drivers.Configurator
	dataSourceName string
	// DB holds the raw *sql.DB for direct access.
	// Errors produced by calling DB directly are not monitored by this package.
	DB            *sql.DB
	reconnectWait time.Duration
	mtx           sync.RWMutex
}

func newNode(conf drivers.Configurator, dsn string, statsLen, maxFails int, reconnectWait time.Duration) *Node {
	return &Node{
		nodeStats:      newNodeStats(statsLen, maxFails),
		Configurator:   conf,
		dataSourceName: dsn,
		reconnectWait:  reconnectWait,
	}
}

// Open calls sql.Open() with the configured driverName and dataSourceName.
// Open should only be used after the Node was (auto-)closed and reconnection is disabled.
func (n *Node) Open() (err error) {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	if n.DB != nil {
		return errors.New(ErrAlreadyOpen)
	}

	n.DB, err = sql.Open(n.DriverName(), n.dataSourceName)
	n.reset()

	return err
}

// Close the current node and make it unavailable
func (n *Node) Close() error {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	var err error
	if n.DB == nil {
		err = sql.ErrConnDone
	} else {
		err = n.DB.Close()
	}

	n.DB = nil
	return err
}

// InUse get the InUse counter from db.Stats.
// Returns -1 in case db is unavailable.
func (n *Node) InUse() int {
	n.mtx.RLock()
	defer n.mtx.RUnlock()

	if n.DB == nil {
		return -1
	}
	return n.DB.Stats().InUse
}

// CheckErr updates the statistics. If the error is nil or whitelisted, success is recorded.
// Any other case constitutes an error and failure is recorded.
//
// This method is already called by each database call method and need to be used in most cases.
// It is exported for use in extending libraries which need use struct embedding
// and want to overload Node methods, while still keeping statistics up-to-date.
func (n *Node) CheckErr(err error) error {
	switch {
	case err == nil:
		go n.failed(false)
	case errors.Is(err, sql.ErrNoRows) || errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled):
		go n.failed(false)
	case n.WhiteList(err):
		go n.failed(false)
	default:
		go n.failed(true)
	}
	return err
}

// ExecContext wrapper around sql.DB.Exec.
func (n *Node) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	n.mtx.RLock()
	res, err := n.DB.ExecContext(ctx, query, args...)
	n.mtx.RUnlock()

	return res, n.CheckErr(err)
}

// Exec wrapper around sql.DB.ExecContext,
// using context.Background().
func (n *Node) Exec(query string, args ...interface{}) (sql.Result, error) {
	return n.ExecContext(context.Background(), query, args...)
}

// QueryContext wrapper around sql.DB.QueryContext.
func (n *Node) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	n.mtx.RLock()
	rows, err := n.DB.QueryContext(ctx, query, args...)
	n.mtx.RUnlock()

	return rows, n.CheckErr(err)
}

// Query wrapper around sql.DB.QueryRowContext,
// using context.Background().
func (n *Node) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return n.QueryContext(context.Background(), query, args...)
}

// QueryRowContext wrapper around sql.DB.QueryRowContext.
func (n *Node) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	n.mtx.RLock()
	row := n.DB.QueryRowContext(ctx, query, args...)
	n.mtx.RUnlock()

	n.CheckErr(row.Err())

	return row
}

// QueryRow wrapper around sql.DB.QueryRowContext,
// using context.Background().
func (n *Node) QueryRow(query string, args ...interface{}) *sql.Row {
	return n.QueryRowContext(context.Background(), query, args...)
}

// BUG(muhlemmer): Node types do not implement boil.Beginner
// https://github.com/moapis/multidb/issues/2

// BeginTx opens a new *sql.Tx on the Node.
func (n *Node) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	n.mtx.RLock()
	tx, err := n.DB.BeginTx(ctx, opts)
	n.mtx.RUnlock()

	return &Tx{n, tx}, n.CheckErr(err)
}

// Begin opens a new *sql.Tx on the Node.
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
		if n != nil && n.DB != nil {
			st := n.DB.Stats()
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

	if len(ent) < max || max == 0 {
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

// readOnlyOpts sets TxOptions.ReadOnly to true.
// If opts is nil, a new one will be initialized.
func readOnlyOpts(opts *sql.TxOptions) *sql.TxOptions {
	if opts == nil {
		opts = new(sql.TxOptions)
	}
	opts.ReadOnly = true
	return opts
}
