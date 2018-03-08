package database

import (
	"github.com/Mitu217/tamate/schema"
)

type Database interface {
	Dump(*schema.Schema) error
	Restore(*schema.Schema, [][]string) error
}
