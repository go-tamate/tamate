package exporter

import (
	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/differ"
)

const (
	DiffDirectionLeftToRight = iota
	DiffDirectionRightToLeft
)

type DiffDirection int

type Exporter interface {
	ExportStruct(left datasource.Datasource, right datasource.Datasource) (*differ.Diff, error)
	SetDirection(diffDirection DiffDirection)
}
