# tamate

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GoDoc](https://godoc.org/github.com/Mitu217/tamate?status.svg)](https://godoc.org/github.com/Mitu217/tamate)
[![Go Report Card](https://goreportcard.com/badge/github.com/Mitu217/tamate)](https://goreportcard.com/report/github.com/Mitu217/tamate)

[![CircleCI](https://circleci.com/gh/Mitu217/tamate.svg?style=svg)](https://circleci.com/gh/Mitu217/tamate)

A library to handle table-based data generically.

---------------------------------------

  * [Features](#features)
  * [Requirements](#requirements)
  * [Support Drivers](#support-drivers)
  * [Installation](#installation)
  * [Usage](#usage)
    * [DataSource](#datasource)
      * [DSN](#dsn-data-source-name)
  * [Testing / Development](#testing--development)
  * [License](#license)

---------------------------------------

## Features
 * Unification of ambiguous names like `left/right`
 * Goroutine safe
 * GetRows returns iterator
 * Support TSV
 * Support SQLite
 * Support PostgleSQL

## Requirements
 * Go 1.12 or higher. We aim to support the 3 latest versions of Go.

## Support Drivers
- [CSV](https://github.com/Mitu217/tamate-csv)
- [MySQL](https://github.com/Mitu217/tamate-mysql)
- [Spanner](https://github.com/Mitu217/tamate-spanner)

---------------------------------------

## Installation
Simple install the package to your [$GOPATH](https://github.com/golang/go/wiki/GOPATH "GOPATH") with the [go tool](https://golang.org/cmd/go/ "go command") from shell:
```bash
$ go get -u github.com/Mitu217/tamate
```
Make sure [Git is installed](https://git-scm.com/downloads) on your machine and in your system's `PATH`.

## Usage
_Tamate Driver_ is an implementation of `tamate/driver` interface.

Use `csv` as `driverName` and a valid [DSN](#dsn-data-source-name)  as `dataSourceName`:
```go
import  "github.com/Mitu217/tamate"
import  _ "github.com/Mitu217/tamate-csv"

ds, err := tamate.Open("csv", "./")
```

### DataSource

DataSource represents the connection destination where table-based data supported by _Tamate_.

Use this to `Get`, `Set`, `GettingDiff`, etc.

#### DSN (Data Source Name)

[DSN](#dsn-data-source-name) is not only common format such as used in `database/sql`.

Please refer to the usage of the driver to use.

## Testing / Development

Please execute the following command at the root of the project

```bash
go test ./...
```

---------------------------------------

## License
* MIT
    * see [LICENSE](./LICENSE)