// Copyright (c) 2020, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"context"
	"database/sql"
)

// IsMaster returns a function that executes passed query against a sql.DB.
// The query should return one row, with a single boolean field,
// indicating wether the DB is a master of not. (true when master)
func IsMaster(query string) MasterFunc {
	return func(ctx context.Context, db *sql.DB) (b bool, err error) {
		err = db.QueryRowContext(ctx, query).Scan(&b)
		return
	}
}
