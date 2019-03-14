# tamate

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GoDoc](https://godoc.org/github.com/Mitu217/tamate?status.svg)](https://godoc.org/github.com/Mitu217/tamate)
[![Go Report Card](https://goreportcard.com/badge/github.com/Mitu217/tamate)](https://goreportcard.com/report/github.com/Mitu217/tamate)

[![CircleCI](https://circleci.com/gh/Mitu217/tamate.svg?style=svg)](https://circleci.com/gh/Mitu217/tamate)

Getting diffs between table-based data.

![Summary](https://user-images.githubusercontent.com/3307063/47960958-54827080-e045-11e8-9aaf-6831dbc1cedf.png)

## Support Drivers
- [CSV](https://github.com/Mitu217/tamate-csv)

## Installation

```sh
$ go get -u github.com/Mitu217/tamate
```

## Contribution

### Run tests

```sh
go test ./...
```

### Additional tests

```bash
# For MySQLDatasource test
docker-compose up -d
export TAMATE_MYSQL_DSN=root:example@tcp(localhost:3306)/

# For SpannerDatasource test
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/spanner_credentials.json
export TAMATE_SPANNER_DSN_PARENT=/projects/<GCP_PROJECT_ID>/instances/<SPANNER_INSTANCE_ID>

# For SpreadsheetDatasource test
export TAMATE_SPREADSHEET_SERVICE_ACCOUNT_JSON_BASE64=<base64 encoded service account key JSON>
```
