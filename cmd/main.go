package main

import (
	"context"
	"l0/internal/config"
	"l0/internal/ns"
	"l0/internal/psql"
	"l0/internal/service"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	//ns_url := "localhost:4223"
	//psql_url := "host=localhost port=5432 user=my_username password=123 dbname=my_db sslmode=disable"

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	//ns, err := ns.New(ns_url)
	//if err != nil {
	//	slog.Error("Creating NATS Streaming", "Error", err.Error())
	//	return
	//}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	//go func() {
	//	for {
	//		select {
	//		case <-ctx.Done():
	//			fmt.Println("Closing...")
	//			err := ns.Close()
	//			if err != nil {
	//				slog.Error("Closing NATS Streaming:", "Error", err.Error())
	//			}
	//			os.Exit(0)
	//
	//		}
	//	}
	//}()

	cfg, err := config.Read(ctx)
	if err != nil {
		slog.Error("Reading configuration", "Error", err.Error())
		return
	}

	nsConn, err := ns.New(ctx, cfg.NS.ConnectionURL())
	if err != nil {
		slog.Error("Connecting NATS Streaming", "Error", err.Error())
		return
	}

	psqlConnURL, err := cfg.Postgres.ConnectionURL()
	if err != nil {
		slog.Error("Getting PSQL Connection URL", "Error", err.Error())
		return
	}
	psqlConn, err := psql.New(ctx, psqlConnURL, time.Second*time.Duration(cfg.Postgres.ConnTimeout))
	if err != nil {
		slog.Error("Connecting PSQL", "Error", err.Error())
		return
	}

	srv := service.New(nsConn, psqlConn)
	err = srv.Start(ctx)
	if err != nil {
		slog.Error("Starting service", "Error", err.Error())
		return
	}

	//ns.PublishTestData()
	//ns.GetMessage()

}
