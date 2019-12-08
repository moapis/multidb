// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package postgresql

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/lib/pq"
)

// Node holds the address and port information for a single DB Node
type Node struct {
	Host string `json:"host,omitempty"` // The host to connect to. Values that start with / are for unix domain sockets. (default is localhost)
	Port uint16 `json:"port,omitempty"` // The port to bind to. (default is 5432)
}

// SSLMode used for connection
type SSLMode string

const (
	// SSLDisable means: No SSL
	SSLDisable SSLMode = "disable"
	// SSLRequire means: Always SSL
	// (skip verification)
	SSLRequire SSLMode = "require"
	// SSLVerifyCA means: Always SSL
	// (verify that the certificate presented by the server was signed by a trusted CA)
	SSLVerifyCA SSLMode = "verify-ca"
	// SSLVerifyFull means: Always SSL
	// (verify that the certification presented by the server was signed by a trusted CA
	// and the server host name matches the one in the certificate)
	SSLVerifyFull SSLMode = "verify-full"
)

// Params holds all remaining pq settings, as defined at https://godoc.org/github.com/lib/pq#hdr-Connection_String_Parameters
type Params struct {
	DBname                    string  `json:"dbname,omitempty"`                    // The name of the database to connect to
	User                      string  `json:"user,omitempty"`                      // The user to sign in as
	Password                  string  `json:"password,omitempty"`                  // The user's password
	SSLmode                   SSLMode `json:"sslmode,omitempty"`                   // Whether or not to use SSL (default is require, this is not the default for libpq)
	Fallback_application_name string  `json:"fallback_application_name,omitempty"` // An application_name to fall back to if one isn't provided.
	Connect_timeout           uint    `json:"connect_timeout,omitempty"`           // Maximum wait for connection, in seconds. Zero or not specified means wait indefinitely.
	SSLcert                   string  `json:"sslcert,omitempty"`                   // Cert file location. The file must contain PEM encoded data.
	SSLkey                    string  `json:"sslkey,omitempty"`                    // Key file location. The file must contain PEM encoded data.
	SSLrootcert               string  `json:"sslrootcert,omitempty"`               // The location of the root certificate file. The file must contain PEM encoded data.
}

func connString(d interface{}) string {
	v := reflect.ValueOf(d)
	t := v.Type()

	var str []string
	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).IsZero() {
			str = append(str,
				strings.Join([]string{
					strings.ToLower(t.Field(i).Name),
					fmt.Sprint(v.Field(i).Interface()),
				}, "="),
			)
		}
	}
	return strings.Join(str, " ")
}

// Config for postgreSQL drivers. Multiple Nodes will use the same set of params.
type Config struct {
	Nodes  []Node `json:"nodes,omitempty"`
	Params Params `json:"params,omitempty"`
}

// DataSourceNames implements driver.Configurator
func (c Config) DataSourceNames() (dsns []string) {
	ps := connString(c.Params)
	for _, n := range c.Nodes {
		dsns = append(dsns, strings.Join([]string{connString(n), ps}, " "))
	}
	return
}

const (
	// DriverName for postgres driver
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
