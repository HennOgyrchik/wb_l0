package main

import (
	"context"
	"fmt"
	"l0/internal/cache"
	"l0/internal/config"
	"l0/internal/ns"
	"l0/internal/psql"
	"l0/internal/service"
	"l0/internal/web"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	nsConn, psqlConn, webSrv, cacheObj, err := prepare(ctx)
	if err != nil {
		slog.Error("Prepare connections", "Error", err.Error())
		return
	}
	srv, err := service.New(ctx, nsConn, psqlConn, cacheObj)
	if err != nil {
		slog.Error("Creating new service", "Error", err.Error())
		return
	}

	var wg sync.WaitGroup

	errChan := make(chan error)
	defer close(errChan)

	wg.Add(1)
	go srv.Start(ctx, &wg, errChan)

	wg.Add(1)
	go webSrv.Start(ctx, srv, &wg, errChan)

	select {
	case err := <-errChan:
		slog.Error(err.Error())
		fmt.Println("Стопаю")
		stop()
	case <-ctx.Done():

	}

	wg.Wait()
	fmt.Println("Closing connections...")

	err = srv.Stop()
	if err != nil {
		slog.Error("Closing app:", "Error", err.Error())
	}
}

func prepare(ctx context.Context) (nsConn *ns.NS, psqlConn *psql.PSQL, webSrv web.Web, cacheObj *cache.Cache, err error) {
	cfg, err := config.Read(ctx)
	if err != nil {
		err = fmt.Errorf("Reading configuration: %w", err)
		return
	}

	nsConn, err = ns.New(ctx, cfg.NS.ConnectionURL())
	if err != nil {
		err = fmt.Errorf("Connecting NATS Streaming: %w", err)
		return
	}

	psqlConnURL, err := cfg.Postgres.ConnectionURL()
	if err != nil {
		err = fmt.Errorf("Getting PSQL Connection URL: %w", err)
		return
	}
	psqlConn, err = psql.New(ctx, psqlConnURL, time.Second*time.Duration(cfg.Postgres.ConnTimeout))
	if err != nil {
		err = fmt.Errorf("Connecting PSQL: %w", err)
		return
	}

	webSrv = web.New(cfg.Web.ConnectionURL())

	cacheObj = cache.New()

	return
}
