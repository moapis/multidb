// Copyright (c) 2020, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"
)

// IsMaster returns a MasterFunc that executes passed query against a sql.DB.
// The query should return one row, with a single boolean field,
// indicating wether the DB is a master of not. (true when master)
func IsMaster(query string) MasterFunc {
	return func(ctx context.Context, db *sql.DB) (b bool, err error) {
		err = db.QueryRowContext(ctx, query).Scan(&b)
		return
	}
}

// DefaultWhiteListErrs are errors which do not signal a DB or connection error.
var DefaultWhiteListErrs = []error{
	sql.ErrNoRows,
	sql.ErrTxDone,
	context.Canceled,
}

// AutoMasterSelector provides means of automatically running SelectMaster
// in case of errors on the Master node.
type AutoMasterSelector struct {
	mdb       *MultiDB
	timeout   time.Duration
	whiteList []error

	closed chan struct{}

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

// AutoMasterSelector returned will run mdb.SelectMaster up to once in an interval,
// after CheckMaster is called with an error not in whiteList.
//
// If whitList errors are ommited, DefaultWhiteListErrs will be used.
func (mdb *MultiDB) AutoMasterSelector(onceIn time.Duration, whiteList ...error) *AutoMasterSelector {
	if len(whiteList) == 0 {
		whiteList = DefaultWhiteListErrs
	}

	// Prevent recursion when SelectMaster is the origin of the error
	whiteList = append(whiteList, ErrNoNodes)

	// Make ctx non-nil, but it should be Done
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return &AutoMasterSelector{
		mdb:       mdb,
		timeout:   onceIn,
		whiteList: whiteList,
		closed:    make(chan struct{}),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Close cleans up any running Go routines.
// Calling MasterErr after Close becomes a no-op.
func (ms *AutoMasterSelector) Close() {
	defer func() {
		recover()
	}()

	close(ms.closed)
	ms.cancel()
}

func (ms *AutoMasterSelector) selectMaster() {
	ms.mu.Lock()

	select {

	case <-ms.ctx.Done():

		ms.ctx, ms.cancel = context.WithTimeout(context.Background(), ms.timeout)
		ms.mu.Unlock()

		ms.mdb.SelectMaster(ms.ctx)

	default:

		ms.mu.Unlock()

		// Don't run SelectMaster if context was not yet expired
		return
	}
}

// CheckErr checks if the passed error is in WhiteListErrs or nil.
// If it is neither, mdb.SelectMaster is executed.
// The returned error is always the original error, which may be nil.
func (ms *AutoMasterSelector) CheckErr(err error) error {
	if err == nil {
		return err
	}

	// after closed, no-op
	select {
	case <-ms.closed:
		return err
	default:
	}

	for _, target := range ms.whiteList {
		if errors.Is(err, target) {
			return err
		}
	}

	// Prevent recursion when SelectMaster is the origin of the error
	if target := new(NoMasterErr); errors.As(err, &target) {
		return err
	}

	// Run in Go routine, so the caller won't be blocked during selection
	go ms.selectMaster()

	return err
}
