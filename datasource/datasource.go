package datasource

import "io"

type DataSource interface {
	Output(dst DataSource, w io.Writer) error
}