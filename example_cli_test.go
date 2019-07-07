package tamate_test

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/go-tamate/tamate"
)

var pool *tamate.DataSource

func Example_openDBCLI() {
	name := flag.String("name", "", "table name to find")
	dsn := flag.String("dsn", os.Getenv("DSN"), "connection data source name")
	flag.Parse()

	if len(*dsn) == 0 {
		log.Fatal("missing dsn flag")
	}
	var err error

	pool, err = tamate.Open("driver-name", *dsn)
	if err != nil {
		log.Fatal("unable to use data source name", err)
	}
	defer pool.Close()

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	appSignal := make(chan os.Signal, 3)
	signal.Notify(appSignal, os.Interrupt)

	go func() {
		select {
		case <-appSignal:
			stop()
		}
	}()

	GetRows(ctx, *name)
}

func GetRows(ctx context.Context, name string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := pool.GetRows(ctx, name)
	if err != nil {
		log.Fatal(err)
	}
}
