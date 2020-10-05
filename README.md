[![Build Status](https://travis-ci.org/moapis/multidb.svg?branch=master)](https://travis-ci.org/moapis/multidb)
[![codecov](https://codecov.io/gh/moapis/multidb/branch/master/graph/badge.svg)](https://codecov.io/gh/moapis/multidb)
[![GoDoc](https://godoc.org/github.com/moapis/multidb?status.svg)](https://godoc.org/github.com/moapis/multidb)
[![Go Report Card](https://goreportcard.com/badge/github.com/moapis/multidb)](https://goreportcard.com/report/github.com/moapis/multidb)


# Multidb

Package multidb provides a `sql.DB` multiplexer for parallel queries using Go routines.

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

`Beta` stage:
 - The intended design is fully implemented and unit tested with the race detector.
 - We are using this library in other projects and it is being tested for production


## Dependencies

This package has been developed against Go 1.15, with module support and will not compile against older Go versions.
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