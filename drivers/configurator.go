package drivers

// Configurator is used to configure different database flavors
type Configurator interface {
	// DriverName for all nodes.
	DriverName() string
	// DataSourceNames returns a DataSourceName
	// (aka connection string) for each DB Node.
	DataSourceNames() []string
}
