// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

/*
Package multidb provides a sql.DB multiplexer for parallel queries using Go routines.

Multidb automatically polls which of the connected Nodes is a master.
Actual management of master and slaves (such as promotion)
is considered outside the scope of this package.

The Node and MultiNode types aim to be interface compatible with sql.DB and sql.Tx.
More specifically, multidb fully implements SQLBoiler's boil.Executor and
boil.ContextExecutor interface types.
This makes it an excellent fit for SQLBoiler.
(And perhaps other ORMs?)
*/
package multidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
)

const errNoMaster = "No master: %v"

// NoMasterErr is returned when there is no master available.
// The causing error is wrapped and can be obtained through errors.Unwrap().
type NoMasterErr struct {
	wrapped error
}

func (err *NoMasterErr) Error() string {
	return fmt.Sprintf(errNoMaster, err.wrapped)
}

func (err *NoMasterErr) Unwrap() error {
	return err.wrapped
}

var (
	// ErrNoNodes is returned when there are no connected nodes available for the requested operation
	ErrNoNodes = errors.New("No available nodes")
)

// MultiDB holds the multiple DB objects, capable of Writing and Reading.
type MultiDB struct {
	nm nodeMap

	// MasterFunc should return true if the passed DB is the Master,
	// typically by executing a driver specific query.
	// False should be returned in all other cases.
	// In case the query fails, an error should be returned.
	MasterFunc func(context.Context, *sql.DB) (bool, error)

	// holds the master node
	master atomic.Value
}

// Close the DB connectors on all nodes.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
func (mdb *MultiDB) Close() error {
	nodes := mdb.nm.getList()
	ec := make(chan error, len(nodes))

	for _, n := range nodes {
		go func(n *Node) {
			ec <- n.Close()
		}(n)
	}

	var errs []error

	for i := 0; i < len(nodes); i++ {
		if err := <-ec; err != nil {
			errs = append(errs, err)
		}
	}

	return checkMultiError(errs)
}

// Add DB Nodes to MultiDB
func (mdb *MultiDB) Add(nodes ...*Node) {
	mdb.nm.add(nodes...)
}

// Delete DB Nodes from MultiDB, identified by names
func (mdb *MultiDB) Delete(names ...string) {
	mdb.nm.delete(names...)
}

// availableNodes returns a list of nodes,
// sorted by usage factor.
// If there are no nodes available, ErrNoNodes is returned.
func (mdb *MultiDB) availableNodes(max int) ([]*Node, error) {
	nodes := mdb.nm.sortedList()
	if len(nodes) == 0 {
		return nil, ErrNoNodes
	}

	if max == 0 || max > len(nodes) {
		return nodes, nil
	}

	return nodes[:max], nil
}

// SelectMaster runs mdb.MasterFunc against all Nodes.
// The first one to return `true` will be returned and stored as master
// for future calles to `mdb.Master()`.
//
// This method should be called only if trouble is detected with the current master,
// which might mean that the master role is shifted to another Node.
func (mdb *MultiDB) SelectMaster(ctx context.Context) (*Node, error) {
	type result struct {
		node     *Node
		err      error
		isMaster bool
	}

	nodes := mdb.nm.getList()

	if len(nodes) == 0 {
		return nil, ErrNoNodes
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rc := make(chan result, len(nodes))

	for _, n := range nodes {
		go func(n *Node) {
			res := result{
				node: n,
			}

			res.isMaster, res.err = mdb.MasterFunc(ctx, n.DB)

			rc <- res
		}(n)
	}

	var errs []error

	for i := 0; i < len(nodes); i++ {
		res := <-rc

		if res.isMaster {
			mdb.master.Store(res.node)
			return res.node, nil
		}

		if res.err != nil {
			errs = append(errs, res.err)
		}
	}

	return nil, &NoMasterErr{checkMultiError(errs)}
}

// Master node getter.
//
// If there is no Master set, like after initialization,
// SelectMaster is ran to find the apropiate Master node.
func (mdb *MultiDB) Master(ctx context.Context) (*Node, error) {
	if master, ok := mdb.master.Load().(*Node); ok {
		return master, nil
	}

	return mdb.SelectMaster(ctx)
}

// MasterTx returns the master node with an opened transaction
func (mdb *MultiDB) MasterTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	master, err := mdb.Master(ctx)
	if err != nil {
		return nil, err
	}

	return master.BeginTx(ctx, opts)
}

// Node returns any ready Mode with the lowest value after
// division of InUse/MaxOpenConnections.
//
// The returned node may be master or slave and should
// only be used for read operations.
func (mdb *MultiDB) Node() (*Node, error) {
	nodes, err := mdb.availableNodes(1)
	if err != nil {
		return nil, err
	}

	return nodes[0], nil
}

// NodeTx returns any node with an opened transaction.
// The transaction is created in ReadOnly mode.
func (mdb *MultiDB) NodeTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	node, err := mdb.Node()
	if err != nil {
		return nil, err
	}
	return node.BeginTx(ctx, readOnlyOpts(opts))
}

// All returns all Nodes.
func (mdb *MultiDB) All() []*Node {
	return mdb.nm.getList()
}

// MultiNode returns available *Nodes.
// Nodes are sorted by the division of InUse/MaxOpenConnections.
// Up to `max` amount of nodes will be in the returned object.
// If `max` is set to 0, all available nodes are returned.
// An error is returned in case no nodes are available.
//
// The nodes may be master or slaves and should
// only be used for read operations.
func (mdb *MultiDB) MultiNode(max int) (MultiNode, error) {
	return mdb.availableNodes(max)
}

// MultiTx returns a MultiNode with an open transaction
func (mdb *MultiDB) MultiTx(ctx context.Context, opts *sql.TxOptions, max int) (*MultiTx, error) {
	mn, err := mdb.MultiNode(max)
	if err != nil {
		return nil, err
	}
	return mn.BeginTx(ctx, opts)
}
