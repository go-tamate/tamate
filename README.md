# tamate

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GoDoc](https://godoc.org/github.com/Mitu217/tamate?status.svg)](https://godoc.org/github.com/Mitu217/tamate)
[![Go Report Card](https://goreportcard.com/badge/github.com/Mitu217/tamate)](https://goreportcard.com/report/github.com/Mitu217/tamate)

[![CircleCI](https://circleci.com/gh/Mitu217/tamate.svg?style=svg)](https://circleci.com/gh/Mitu217/tamate)

Getting diffs between table-based data.

---------------------------------------

  * [Features](#features)
  * [Requirements](#requirements)
  * [Support Drivers](#support-drivers)
  * [Installation](#installation)
  * [Usage](#usage)
    * [DataSource](#datasource)
    * [Diff](#dsn-data-source-name)
      * [Options](#diff-options)
      * [Examples](#examples)
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
_Go MySQL Driver_ is an implementation of Go's `database/sql/driver` interface. You only need to import the driver and can use the full [`database/sql`](https://golang.org/pkg/database/sql/) API then.

Use `mysql` as `driverName` and a valid [DSN](#dsn-data-source-name)  as `dataSourceName`:
```go
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

db, err := sql.Open("mysql", "user:password@/dbname")
```

[Examples are available in our Wiki](https://github.com/go-sql-driver/mysql/wiki/Examples "Go-MySQL-Driver Examples").

### DataSource

TODO:

### Diff

TODO:

#### Options

TODO:

#### Examples

TODO:

## Testing / Development
To run the driver tests you may need to adjust the configuration. See the [Testing Wiki-Page](https://github.com/go-sql-driver/mysql/wiki/Testing "Testing") for details.

Go-MySQL-Driver is not feature-complete yet. Your help is very appreciated.
If you want to contribute, you can work on an [open issue](https://github.com/go-sql-driver/mysql/issues?state=open) or review a [pull request](https://github.com/go-sql-driver/mysql/pulls).

See the [Contribution Guidelines](https://github.com/go-sql-driver/mysql/blob/master/CONTRIBUTING.md) for details.

---------------------------------------

## License
* MIT
    * see [LICENSE](./LICENSE)