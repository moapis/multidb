// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

// Package postgresql defines constants for using PostgreSQL databases with MultiDB.
package postgresql

const (
	// MasterQuery returns true when not in recovery (means node is master)
	MasterQuery = "select not pg_is_in_recovery();"
)
