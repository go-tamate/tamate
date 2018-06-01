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

func (dd DiffDirection) String() string {
	switch dd {
	case DiffDirectionRightToLeft:
		return "RIGHT_TO_LEFT"
	case DiffDirectionLeftToRight:
		fallthrough
	default:
		return "LEFT_TO_RIGHT"
	}
}

type Exporter interface {
	SetDatasources(left, right datasource.Datasource)
	SetDirection(diffDirection DiffDirection)
	ExportStruct() (*differ.Diff, error)
}
