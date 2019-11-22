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
}
