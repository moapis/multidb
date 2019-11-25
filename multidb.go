// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/moapis/multidb/drivers"
)

const (
	// ErrNoNodes is returned when there are no connected nodes available for the requested operation
	ErrNoNodes = "No available nodes"
	// ErrNoMaster is returned when no master is available
	ErrNoMaster = "No available master"
	// ErrSuccesReq is returned when higher than 1.0
	ErrSuccesReq = "SuccesReq > 1"
)

// Config configures multiple databas servers
type Config struct {
	DBConf drivers.Configurator

	// Amount of past connections to consider when establishing the failure rate.
	StatsLen int
	// Amount of allowed counted failures, after which the DB connector will be closed.
	// Note that Go's SQL connectors are actually connection pools.
	// Individual connections are already reset upon connection errors by the sql library.
	// This library closes the complete pool for a single node.
	// 0 disconnects on the first error. (Probably not what you want)
	// A value >= StatsLen means 100% failure rate allowed.
	// Negative values disables autoclosing statistics / counting.
	MaxFails int
	// Time to wait before attempting to reconnect failed nodes.
	// Attemps will be done indefinitly.
	// Set to 0 to disable reconnects.
	ReconnectWait time.Duration
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
	driverName := c.DBConf.DriverName()
	dataSourceNames := c.DBConf.DataSourceNames()

	if len(dataSourceNames) == 0 {
		return nil, errors.New(ErrNoNodes)
	}

	mdb := new(MultiDB)
	mdb.all = make([]*Node, len(dataSourceNames))

	for i, dsn := range dataSourceNames {
		mdb.all[i] = newNode(driverName, dsn, c.StatsLen, c.MaxFails, c.ReconnectWait)
		if err := mdb.all[i].Open(); err != nil {
			go mdb.all[i].reconnect()
		}
	}
	return mdb, nil
}

func (mdb *MultiDB) setMaster(ctx context.Context) (*Node, error) {
	mdb.mtx.Lock()
	defer mdb.mtx.Unlock()

	switch len(mdb.all) {
	case 0:
		return nil, errors.New(ErrNoNodes)
	case 1:
		mdb.master = mdb.all[0]
		return mdb.master, nil
	}
	// TODO: some queries
	return nil, errors.New(ErrNoMaster)
}

// Master node getter
func (mdb *MultiDB) Master(ctx context.Context) (*Node, error) {
	mdb.mtx.RLock()
	master := mdb.master
	mdb.mtx.RUnlock()

	if db := master.DB(); db == nil {
		return mdb.setMaster(ctx)
	}
	return master, nil
}

// MasterTx returns the master node with an opened transaction
func (mdb *MultiDB) MasterTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	master, err := mdb.Master(ctx)
	if err != nil {
		return nil, err
	}
	return master.BeginTx(ctx, opts)
}

// Node returns any Ready node with the lowest usage counter
// The returned node may be master or slave and should
// only be used for read operations.
func (mdb *MultiDB) Node() (*Node, error) {
	mdb.mtx.RLock()
	defer mdb.mtx.RUnlock()

	var node *Node
	var use int
	for _, n := range mdb.all {
		u := n.InUse()
		if node == nil || (u >= 0 && u < use) {
			node, use = n, u
		}
	}
	if node == nil {
		return nil, errors.New(ErrNoNodes)
	}
	return node, nil
}

// NodeTx returns any node with an opened transaction
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
