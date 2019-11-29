// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package postgresql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lib/pq"
)

// connString build the connection string for host with params
func connString(host Host, params map[string]string) string {
	parts := make([]string, 0, len(params)+2)
	if host.Addr != "" {
		parts = append(parts, fmt.Sprintf("host=%s", host.Addr))
	}
	if host.Port != 0 {
		parts = append(parts, fmt.Sprintf("port=%d", host.Port))
	}
	// Sorting of map needed for unit tests
	// Also, filters out empty entries
	keys := make([]string, 0, len(params))
	for k, v := range params {
		if v != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, params[key]))
	}
	return strings.Join(parts, " ")
}

// Host network information
type Host struct {
	// IP address, hostname or socket location
	Addr string
	// TCP port
	Port uint16
}

// Config for postgreSQL drivers
type Config struct {
	Hosts []Host
	// Params are parsed as `k=v` and appended to the connection string
	Params map[string]string
}

// DataSourceNames implements driver.Configurator
func (c Config) DataSourceNames() (snds []string) {
	for _, host := range c.Hosts {
		snds = append(snds, connString(host, c.Params))
	}
	return
}

const (
	// DriverName for postgres drivers
	DriverName = "postgres"
	// MasterQuery returns true when not in recovery (means node is master)
	MasterQuery = "select not pg_is_in_recovery();"
)

// DriverName returns the DriverName constant.
// Implements driver.Configurator
func (Config) DriverName() string {
	return DriverName
}

// MasterQuery returns the MasterQuery constant.
// Implements driver.Configurator
func (Config) MasterQuery() string {
	return MasterQuery
}

var (
	blackListClasses = []string{
		"08", // Connection Exception
		"3B", // Savepoint Exception
		"53", // Insufficient Resources
		"57", // Operator Intervention
		"58", // System Error (errors external to PostgreSQL itself)
		"F0", // Configuration File Error
		"XX", // Internal Error
	}
)

// WhiteList returns true if the passed error is not a connection on DB consistency error.
// It used pq error codes for checking
func (Config) WhiteList(err error) bool {
	if err == nil {
		return true
	}
	pqe, ok := err.(pq.Error)
	if !ok {
		return false
	}
	class := string(pqe.Code.Class())
	for _, blc := range blackListClasses {
		if class == blc {
			return false
		}
	}
	return true
}
