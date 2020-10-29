package multidb

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sync"
)

func ExampleErrCallbackFunc() {
	var (
		// ErrCallBackFunc is called concurrently.
		// A Mutex is required when accessing a shared object
		mu       sync.Mutex
		counters map[string]int
	)

	ecb := func(err error) {

		// Ignore unimportant errors
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, sql.ErrNoRows) ||
			errors.Is(err, sql.ErrTxDone) {
			return
		}

		nodeErr := new(NodeError)

		if !errors.As(err, &nodeErr) {
			log.Printf("Unknown error: %v", err)
		}

		mu.Lock()

		counters[nodeErr.name]++
		log.Printf("%v; fail count: %d", nodeErr, counters[nodeErr.name])

		mu.Unlock()
	}

	mdb := new(MultiDB)
	_, _ = mdb.MultiNode(3, ecb)
	// Continue running queries
}
