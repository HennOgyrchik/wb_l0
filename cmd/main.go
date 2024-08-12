package main

import (
	"context"
	"fmt"
	"l0/internal/config"
	"l0/internal/ns"
	"l0/internal/psql"
	"l0/internal/service"
	"l0/internal/web"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	err = web.Start(cfg.Web.ConnectionURL())
	if err != nil {
		slog.Error("Starting web-server", "Error", err.Error())
		return
	}

	srv, err := service.New(ctx, nsConn, psqlConn)
	if err != nil {
		slog.Error("Creating new service", "Error", err.Error())
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Closing...")
				err := srv.Stop()
				if err != nil {
					slog.Error("Closing app:", "Error", err.Error())
				}
				os.Exit(0)

			}
		}
	}()

	err = srv.Start(ctx)
	if err != nil {
		slog.Error("Starting service", "Error", err.Error())
		return
	}
}
