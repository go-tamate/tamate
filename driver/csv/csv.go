package csv

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
)

func readFromFile(rootDir, fileName string) ([][]string, error) {
	path := joinPath(rootDir, fileName)
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return read(r)
}

func read(r io.Reader) ([][]string, error) {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1
	values, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	return values, err
}

func writeToFile(rootDir, fileName string, values [][]string) error {
	path := joinPath(rootDir, fileName)
	w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer w.Close()
	return write(w, values)
}

func write(w io.Writer, values [][]string) error {
	return csv.NewWriter(w).WriteAll(values)
}

func delete(rootDir, fileName string) error {
	path := joinPath(rootDir, fileName)
	return os.Remove(path)
}

func joinPath(rootDir, fileName string) string {
	return filepath.Join(rootDir, fileName+".csv")
}
