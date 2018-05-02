package config

// SQLTableConfig is table config struct of sql.
type SQLTableConfig struct {
	DriverName string `json:"driver_name"`
	DSN        string `json:"dsn"`
	TableName  string `json:"table_name"`
}
