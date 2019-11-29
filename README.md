[![](https://github.com/moapis/multidb/workflows/Go1.13/badge.svg)](https://github.com/moapis/multidb/actions?query=workflow%3A%22Go1.13%22)
[![codecov](https://codecov.io/gh/moapis/multidb/branch/master/graph/badge.svg)](https://codecov.io/gh/moapis/multidb)
[![GoDoc](https://godoc.org/github.com/moapis/multidb?status.svg)](https://godoc.org/github.com/moapis/multidb)
[![Go Report Card](https://goreportcard.com/badge/github.com/moapis/multidb)](https://goreportcard.com/report/github.com/moapis/multidb)


# Multidb

Package multidb provides a `sql.DB` multiplexer for parallel queries using Go routines.
It is meant as a top-level library which connects to a number of database Nodes.
Nodes' health conditions are monitored by inspecting returning errors.
After a (settable) threshold or errors has passed,
a Node is disconnected and considered unavailable for subsequent requests.
Failed nodes can be reconnected automatically.

Multidb automatically polls which of the connected Nodes is a master.
If the master fails, multidb will try to find a new master,
which might be found after promotion took place on a slave
or the old master gets reconnected.
Actual management of master and slaves (such as promotion)
is considered outside the scope of this package.

The Node and MultiNode types aim to be interface compatible with `sql.DB` and `sql.Tx`.
More specifically, multidb fully implements [SQLBoiler's](https://github.com/volatiletech/sqlboiler)
`boil.Executor` and `boil.ContextExecutor` interface types.
This makes it an excellent fit for SQLBoiler's auto-generated models.
(And perhaps other ORMs?)

## Maturity

`Alpha` stage. The intended design is fully implemented and unit tested with the race detector.
Actual implementations using this package still need to prove its stability and usability.

## Dependencies

This package has been developed against Go 1.13, with module support and might not work properly on older Go versions.
The core package is slim and only depends on the standard Go libraries. 
Packages in `drivers/` usually depend on their SQL driver counterpart.
Unit tests pull in some additional packages like `go-sqlmock` and `sqlboiler`.

## Installation

````
go get -u github.com/moapis/multidb
````

Full test suite can be run with:

````
go test ./...
````

## Documentation and examples

[Godoc.org](https://godoc.org/github.com/moapis/multidb)

## Copyright and license

Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
Use of this source code is governed by a License that can be found in the [LICENSE](LICENSE) file.