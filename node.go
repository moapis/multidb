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

	"github.com/moapis/multidb/drivers"
)

const (
	// ErrAlreadyOpen is returned when Opening on an already open Node
	ErrAlreadyOpen = "Node already open"
)

// Node represents a database server connection.
// Node Implements boil.ContextExecutor
type Node struct {
	drivers.Configurator
	dataSourceName string
	// DB holds the raw *sql.DB for direct access.
	// Errors produced by calling DB directly are not monitored by this package.
	DB  *sql.DB
	mtx sync.RWMutex
}

func newNode(conf drivers.Configurator, dsn string, statsLen, maxFails int) *Node {
	return &Node{
		Configurator:   conf,
		dataSourceName: dsn,
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

// ExecContext wrapper around sql.DB.Exec.
func (n *Node) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	n.mtx.RLock()
	res, err := n.DB.ExecContext(ctx, query, args...)
	n.mtx.RUnlock()

	return res, err
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

	return rows, err
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

	return &Tx{tx}, err
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
