// Copyright (c) 2020, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package drivers

import (
	"context"
	"database/sql"
)

// MasterFunc returns a function that executes passed query against a sql.DB.
// The query should return one row, with a single boolean field,
// indicating wether the DB is a master of not. (true when master)
func MasterFunc(query string) func(context.Context, *sql.DB) (bool, error) {
	return func(ctx context.Context, db *sql.DB) (b bool, err error) {
		err = db.QueryRowContext(ctx, query).Scan(&b)
		return
	}
}
