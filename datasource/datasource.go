package datasource

type DataSource interface {
	GetColumns() []string
	SetColumns([]string)
	GetValues() [][]string
	SetValues([][]string)
	Output() error
}
