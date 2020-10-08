// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"database/sql"
	"math"
	"sort"
	"sync"
)

// Node represents a database server connection.
type Node struct {
	name string
	*sql.DB
}

// NewNode with DB, identified by name.
func NewNode(name string, db *sql.DB) *Node {
	return &Node{name, db}
}

// Name of this Node
func (n *Node) Name() string {
	return n.name
}

// stats returns a pointer to sql.DBStats, to avoid copying
func (n *Node) stats() *sql.DBStats {
	s := n.Stats()
	return &s
}

// nodeMap is a map of nodes, identified by names.
// Safe for concurrent use.
type nodeMap struct {
	nodes map[string]*Node

	mtx sync.RWMutex
}

func (m *nodeMap) add(nodes ...*Node) {
	m.mtx.Lock()

	if m.nodes == nil {
		m.nodes = make(map[string]*Node)
	}

	for _, n := range nodes {
		m.nodes[n.name] = n
	}

	m.mtx.Unlock()
}

func (m *nodeMap) delete(names ...string) {
	m.mtx.Lock()

	for _, name := range names {
		delete(m.nodes, name)
	}

	m.mtx.Unlock()
}

// getList returns all Nodes in a nodeList
func (m *nodeMap) getList() nodeList {
	m.mtx.RLock()

	nodes := make([]*Node, len(m.nodes))

	var i int
	for _, node := range m.nodes {
		nodes[i] = node
		i++
	}

	m.mtx.RUnlock()

	return nodes
}

// sortedList returns all Nodes in a nodeList,
// sorted by usage factor.
func (m *nodeMap) sortedList() nodeList {
	nodes := m.getList()
	sort.Sort(nodes)

	return nodes
}

func useFactor(stats *sql.DBStats) float64 {
	f := float64(stats.InUse) / float64(stats.MaxOpenConnections)

	if math.IsNaN(f) {
		return 0
	}

	return f
}

// nodeList implements sort.Interface
type nodeList []*Node

func (l nodeList) Len() int           { return len(l) }
func (l nodeList) Less(i, j int) bool { return useFactor(l[i].stats()) < useFactor(l[j].stats()) }
func (l nodeList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

// readOnlyOpts sets TxOptions.ReadOnly to true.
// If opts is nil, a new one will be initialized.
func readOnlyOpts(opts *sql.TxOptions) *sql.TxOptions {
	if opts == nil {
		opts = new(sql.TxOptions)
	}
	opts.ReadOnly = true
	return opts
}
