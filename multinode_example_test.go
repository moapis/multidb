// Copyright (c) 2020, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"log"
	"time"
)

func ExampleMultiNode() {
	mn, err := mdb.MultiNode(2, func(err error) { log.Println(err) })
	if err != nil {
		panic(err)
	}

	opCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctx, cancel := context.WithCancel(opCtx)
	defer cancel()

	rows, err := mn.QueryContext(ctx, "select $1;", 1)
	if err != nil {
		panic(err)
	}

	var i int

	for rows.Next() {
		if err = rows.Scan(&i); err != nil {
			panic(err)
		}
	}

	// Cancel the context after rows scanning!
	cancel()

	ctx, cancel = context.WithCancel(opCtx)
	err = mn.QueryRowContext(ctx, "select $1", 2).Scan(&i)
	cancel()

	if err != nil {
		panic(err)
	}
}
