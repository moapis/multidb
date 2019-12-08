// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

/*
Package multidb provides a sql.DB multiplexer for parallel queries using Go routines.
It is meant as a top-level library which connects to a number of database Nodes.
Nodes' health conditions are monitored by inspecting returning errors.
After a (settable) threshold or errors has passed,
a Node is disconnected and considered unavailable for subsequent requests.
Failed nodes can be reconnected automatically.

Multidb automatically polls which of the connected Nodes is a master.
If the master fails, multidb will try to find a new master,
which might be found after promotion took place on a slave
or the old master gets reconnected.
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
	"sync"
	"time"

	"github.com/moapis/multidb/drivers"
)

const (
	// ErrNoNodes is returned when there are no connected nodes available for the requested operation
	ErrNoNodes = "No available nodes"
	// ErrNoMaster is returned when no master is available
	ErrNoMaster = "No available master, cause: %w"
	// ErrSuccesReq is returned when higher than 1.0
	ErrSuccesReq = "SuccesReq > 1"
)

// Config configures multiple databas servers
type Config struct {
	DBConf drivers.Configurator `json:"dbconf,omitempty"`

	// Amount of past connections to consider when establishing the failure rate.
	StatsLen int `json:"statslen,omitempty"`
	// Amount of allowed counted failures, after which the DB connector will be closed.
	// Note that Go's SQL connectors are actually connection pools.
	// Individual connections are already reset upon connection errors by the sql library.
	// This library closes the complete pool for a single node.
	// 0 disconnects on the first error. (Probably not what you want)
	// A value >= StatsLen means 100% failure rate allowed.
	// Negative values disables autoclosing statistics / counting.
	MaxFails int `json:"maxfails"`
	// Time to wait before attempting to reconnect failed nodes.
	// Attempts will be done indefinitely.
	// Set to 0 to disable reconnects.
	ReconnectWait time.Duration `json:"reconnectwait"`
}

// MultiDB holds the multiple DB objects, capable of Writing and Reading.
type MultiDB struct {
	master *Node
	all    []*Node
	mtx    sync.RWMutex // Protection for reconfiguration
}

// Open all the configured DB hosts.
// Poll Node.ConnErr() to inspect for connection failures.
// Only returns an error if amount of configured nodes == 0.
//
// If ReconnectWait is set,
// failing Nodes will enter into a reconnection sequence
// and may become available after some time.
func (c Config) Open() (*MultiDB, error) {
	dataSourceNames := c.DBConf.DataSourceNames()
	mdb := &MultiDB{all: make([]*Node, len(dataSourceNames))}
	if len(mdb.all) == 0 {
		return nil, errors.New(ErrNoNodes)
	}

	for i, dsn := range dataSourceNames {
		mdb.all[i] = newNode(c.DBConf, dsn, c.StatsLen, c.MaxFails, c.ReconnectWait)
		if err := mdb.all[i].Open(); err != nil {
			go mdb.all[i].reconnect(context.TODO())
		}
	}
	return mdb, nil
}

// Close the DB connectors on all nodes.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
func (mdb *MultiDB) Close() error {
	mdb.mtx.Lock()
	defer mdb.mtx.Unlock()

	var me MultiError
	for _, n := range mdb.all {
		if err := n.Close(); err != nil {
			me.append(err)
		}
	}
	return me.check()
}

func electMaster(ctx context.Context, nodes []*Node) (*Node, error) {
	type result struct {
		node     *Node
		err      error
		isMaster bool
	}
	var available []*Node
	for _, n := range nodes {
		if n != nil && n.DB != nil {
			available = append(available, n)
		}
	}
	if len(available) == 0 {
		return nil, errors.New(ErrNoNodes)
	}
	rc := make(chan result, len(available))
	for _, n := range available {
		go func(n *Node) {
			res := result{node: n}
			if err := n.QueryRowContext(ctx, n.MasterQuery()).Scan(&res.isMaster); err != nil {
				res.err = n.CheckErr(err) // Should be removed when QueryRowContext becomes error aware
			}
			rc <- res
		}(n)
	}
	var me MultiError
	for i := 0; i < len(available); i++ {
		res := <-rc
		if res.isMaster {
			return res.node, nil
		}
		if res.err != nil {
			me.Errors = append(me.Errors, res.err)
		}
	}
	return nil, me.check()
}

func (mdb *MultiDB) setMaster(ctx context.Context) (*Node, error) {
	mdb.mtx.Lock()
	defer mdb.mtx.Unlock()

	switch len(mdb.all) {
	case 0:
		return nil, fmt.Errorf(ErrNoMaster, errors.New(ErrNoNodes))
	case 1:
		mdb.master = mdb.all[0]
		return mdb.master, nil
	default:
		var err error
		mdb.master, err = electMaster(ctx, mdb.all)
		if err != nil {
			err = fmt.Errorf(ErrNoMaster, err)
		}
		return mdb.master, err
	}
}

// Master node getter
func (mdb *MultiDB) Master(ctx context.Context) (*Node, error) {
	mdb.mtx.RLock()

	if mdb.master == nil { // || !mdb.master.Ready()
		mdb.mtx.RUnlock()
		return mdb.setMaster(ctx)
	}

	defer mdb.mtx.RUnlock()
	return mdb.master, nil
}

// MasterTx returns the master node with an opened transaction
func (mdb *MultiDB) MasterTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
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
	mdb.mtx.RLock()
	defer mdb.mtx.RUnlock()

	nodes, err := availableNodes(mdb.all, 1)
	if err != nil {
		return nil, err
	}
	return nodes[0], nil
}

// NodeTx returns any node with an opened transaction.
func (mdb *MultiDB) NodeTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	node, err := mdb.Node()
	if err != nil {
		return nil, err
	}
	return node.BeginTx(ctx, opts)
}

// All returns all Nodes, regardless of their state.
func (mdb *MultiDB) All() []*Node {
	mdb.mtx.RLock()
	defer mdb.mtx.RUnlock()

	return mdb.all
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
	mdb.mtx.RLock()
	defer mdb.mtx.RUnlock()

	return availableNodes(mdb.all, max)
}

// MultiTx returns a MultiNode with an open transaction
func (mdb *MultiDB) MultiTx(ctx context.Context, opts *sql.TxOptions, max int) (MultiTx, error) {
	mn, err := mdb.MultiNode(max)
	if err != nil {
		return nil, err
	}
	return mn.BeginTx(ctx, opts)
}
