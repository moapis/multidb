// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package drivers

// Configurator is used to configure different database flavors
type Configurator interface {
	// DriverName for all nodes.
	DriverName() string
	// DataSourceNames returns a DataSourceName
	// (aka connection string) for each DB Node.
	DataSourceNames() []string
	// MasterQuery returns a query string which is capable of
	// determining the master state of a DB node.
	// The query should return `true` if the node is master,
	// `false` otherwise.
	MasterQuery() string
	// WhiteList returns true if the passed error is not a connection on DB consistency error.
	// WhiteListed errors will not be counted towards the Node's fail statistics.
	WhiteList(error) bool
}
