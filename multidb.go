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
	"math"
	"sort"
	"sync"
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

// dbSorter implements sort.Interface
type dbSorter struct {
	stats []*sql.DBStats
	names []string
}

func (s *dbSorter) Len() int { return len(s.names) }

func (s *dbSorter) Swap(i, j int) {
	s.stats[i], s.stats[j] = s.stats[j], s.stats[i]
	s.names[i], s.names[j] = s.names[j], s.names[i]
}

func useFactor(stats *sql.DBStats) float64 {
	f := float64(stats.InUse) / float64(stats.MaxOpenConnections)

	if math.IsNaN(f) {
		return 0
	}

	return f
}

func (s *dbSorter) Less(i, j int) bool {
	return useFactor(
		s.stats[i],
	) < useFactor(
		s.stats[j],
	)
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

// MasterFunc should return true if the passed DB is the Master,
// typically by executing a driver specific query.
// False should be returned in all other cases.
// In case the query fails, an error should be returned.
type MasterFunc func(context.Context, *sql.DB) (bool, error)

// MultiDB holds the multiple DB objects, capable of Writing and Reading.
type MultiDB struct {
	nodes      map[string]*sql.DB
	nmu        sync.RWMutex // Protects nodes
	MasterFunc MasterFunc
	master     atomic.Value // holds the master DB node
}

// sortedNames should be run inside a read-locked Mutex
func (mdb *MultiDB) sortedNames() []string {
	dbs := dbSorter{
		stats: make([]*sql.DBStats, len(mdb.nodes)),
		names: make([]string, len(mdb.nodes)),
	}

	var i int

	for name, db := range mdb.nodes {
		s := db.Stats()

		dbs.stats[i] = &s
		dbs.names[i] = name

		i++
	}

	sort.Sort(&dbs)

	return dbs.names
}

// Close the DB connectors on all nodes.
//
// If all nodes respond with the same error, that exact error is returned as-is.
// If there is a variety of errors, they will be embedded in a MultiError return.
func (mdb *MultiDB) Close() error {
	mdb.nmu.RLock()

	n := len(mdb.nodes)
	ec := make(chan error, n)

	for name, db := range mdb.nodes {
		go func(name string, db *sql.DB) {
			err := db.Close()
			if err != nil {
				err = &NodeError{name, err}
			}

			ec <- err
		}(name, db)
	}

	mdb.nmu.RUnlock()

	var errs []error

	for i := 0; i < n; i++ {
		if err := <-ec; err != nil {
			errs = append(errs, err)
		}
	}

	return checkMultiError(errs)
}

// Add DB Node to MultiDB
func (mdb *MultiDB) Add(name string, db *sql.DB) {
	mdb.nmu.Lock()

	if mdb.nodes == nil {
		mdb.nodes = make(map[string]*sql.DB)
	}

	mdb.nodes[name] = db

	mdb.nmu.Unlock()
}

// Delete DB Nodes from MultiDB, identified by names
func (mdb *MultiDB) Delete(names ...string) {
	mdb.nmu.Lock()

	for _, name := range names {
		delete(mdb.nodes, name)
	}

	mdb.nmu.Unlock()
}

// SelectMaster runs mdb.MasterFunc against all Nodes.
// The first one to return `true` will be returned and stored as master
// for future calles to `mdb.Master()`.
//
// This method should be called only if trouble is detected with the current master,
// which might mean that the master role is shifted to another Node.
func (mdb *MultiDB) SelectMaster(ctx context.Context) (*sql.DB, error) {
	type result struct {
		db       *sql.DB
		err      error
		isMaster bool
	}

	mdb.nmu.RLock()

	n := len(mdb.nodes)

	if n == 0 {
		return nil, ErrNoNodes
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rc := make(chan result, n)

	for name, db := range mdb.nodes {
		go func(name string, db *sql.DB) {
			res := result{
				db: db,
			}

			res.isMaster, res.err = mdb.MasterFunc(ctx, db)
			if res.err != nil {
				res.err = &NodeError{name, res.err}
			}

			rc <- res
		}(name, db)
	}

	mdb.nmu.RUnlock()

	var errs []error

	for i := 0; i < n; i++ {
		res := <-rc

		if res.isMaster {
			mdb.master.Store(res.db)
			return res.db, nil
		}

		if res.err != nil {
			errs = append(errs, res.err)
		}
	}

	return nil, &NoMasterErr{checkMultiError(errs)}
}

// Master DB node getter.
//
// If there is no Master set, like after initialization,
// SelectMaster is ran to find the apropiate Master node.
func (mdb *MultiDB) Master(ctx context.Context) (*sql.DB, error) {
	if master, ok := mdb.master.Load().(*sql.DB); ok {
		return master, nil
	}

	return mdb.SelectMaster(ctx)
}

// MasterTx returns the master DB node with an opened transaction
func (mdb *MultiDB) MasterTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	master, err := mdb.Master(ctx)
	if err != nil {
		return nil, err
	}

	return master.BeginTx(ctx, opts)
}

// Node returns any DB Node with the lowest value after
// division of InUse/MaxOpenConnections.
//
// The returned node may be master or slave and should
// only be used for read operations.
func (mdb *MultiDB) Node() (*sql.DB, error) {
	mdb.nmu.RLock()
	defer mdb.nmu.RUnlock()

	if len(mdb.nodes) == 0 {
		return nil, ErrNoNodes
	}

	return mdb.nodes[mdb.sortedNames()[0]], nil
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
func (mdb *MultiDB) All() map[string]*sql.DB {
	mdb.nmu.RLock()

	dbm := make(map[string]*sql.DB, len(mdb.nodes))

	for name, db := range mdb.nodes {
		dbm[name] = db
	}

	mdb.nmu.RUnlock()

	return dbm
}

// MultiNode returns available *Nodes.
// Nodes are sorted by the division of InUse/MaxOpenConnections.
// Up to `max` amount of nodes will be in the returned object.
// If `max` is set to 0, all available nodes are returned.
// An error is returned in case no nodes are available.
//
// The nodes may be master or slaves and should
// only be used for read operations.
func (mdb *MultiDB) MultiNode(max int, errCallback ErrCallbackFunc) (*MultiNode, error) {
	mdb.nmu.RLock()
	defer mdb.nmu.RUnlock()

	if len(mdb.nodes) == 0 {
		return nil, ErrNoNodes
	}

	mn := &MultiNode{
		errCallback: errCallback,
	}

	if max == 0 || max > len(mdb.nodes) {
		mn.nodes = make(map[string]executor, len(mdb.nodes))

		for name, db := range mdb.nodes {
			mn.nodes[name] = db
		}

		return mn, nil
	}

	names := mdb.sortedNames()

	mn.nodes = make(map[string]executor, max)

	for i := 0; i < max; i++ {
		mn.nodes[names[i]] = mdb.nodes[names[i]]
	}

	return mn, nil
}

// MultiTx returns a MultiNode with an open transaction
func (mdb *MultiDB) MultiTx(ctx context.Context, opts *sql.TxOptions, max int, errCallback func(error)) (*MultiTx, error) {
	mn, err := mdb.MultiNode(max, errCallback)
	if err != nil {
		return nil, err
	}
	return mn.BeginTx(ctx, opts)
}
