# tamate

[![CircleCI](https://circleci.com/gh/Mitu217/tamate.svg?style=svg)](https://circleci.com/gh/Mitu217/tamate)
[![Go Report Card](https://goreportcard.com/badge/github.com/Mitu217/tamate)](https://goreportcard.com/report/github.com/Mitu217/tamate)

Reading and Getting diffs between table-based data (CSV, SQL, Google Spreadsheets, etc...)

## Install

```sh
go get github.com/Mitu217/tamate
```

## Usage

### Generate table definition json
Generate template file of table definition

```
tamate generate:config <table type>
```

### Dump
Dump table data to stdout

```
tamate dump <table definition file>
```

### Diff
Show diffs between rows on two tables

```
tamate diff <left table definition file> <right table definition file>
```

## Contribution

### Requirements for development

- [dep](https://github.com/golang/dep)

### Getting started

```sh
go get github.com/Mitu217/tamate
cd $GOPATH/src/github.com/Mitu217/tamate
dep ensure

# Run unit tests
go test ./...
```
