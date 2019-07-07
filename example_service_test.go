package tamate_test

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"time"

	"github.com/go-tamate/tamate"
)

func Example_openTamateService() {
	ds, err := tamate.Open("driver-name", "dsn")
	if err != nil {
		log.Fatal(err)
	}

	s := &Service{ds: ds}
	http.ListenAndServe(":8080", s)
}

type Service struct {
	ds *tamate.DataSource
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ds := s.ds
	switch r.URL.Path {
	default:
		http.Error(w, "not found", http.StatusNotFound)
		return
	case "/get-rows":
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		_, err := ds.GetRows(ctx, "name")
		if err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		return
	}
}
