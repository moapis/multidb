package postgresql

import (
	"fmt"
	"sort"
	"strings"
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

const driverName = "postgres"

// DriverName implements driver.Configurator
func (Config) DriverName() string {
	return driverName
}
