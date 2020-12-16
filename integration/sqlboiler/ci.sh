#!/bin/bash -x 

set -e

go get -v -d ./...
go get -v -u github.com/moapis/multidb@"${TRAVIS_BRANCH:-main}"
go mod tidy

go build
