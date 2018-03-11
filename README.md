# tamate

## Requirements

- [dep](https://github.com/golang/dep)

## Install

```
dep ensure
```

## Commands

### Generate DataSource Config.
```
tamate generate:config -t <datasource type> [-o <ouptut path>]

e.g. tamate generate:config -t SQL -o sql_config.json
```

### Generate Schema.
```
tamate generate:schema -t <datasource type> [-c <config path>] [-o <output path>]

e.g. tamate generate:schema -t SQL -o sql_schema.json
```

### Dump.
```
tamate dump -i <input datasource type> [-o <output datasource path>] <input datasource config path> [<output datasource config path>]

e.g. tamate dump -i SQL sql_schema.json
e.g. tamate dump -i SQL -o SpreadSheets sql_config.json spreadsheets_config.json 
```

### Diff
```
tamate diff -l <input datasource type> -r <output datasource path> [-s <schema path>] <left datasource config path> <right datasource config path>

e.g. tamate dump -l SQL -r SQL sql_config1.json sql_config2.json
e.g. tamate dump -s sql_schema.json -l SQL -r SQL sql_config1.json sql_config2.json
```
