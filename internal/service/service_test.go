package service

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"l0/internal/cache"
	"l0/internal/config"
	"l0/internal/ns"
	"l0/internal/psql"
	"l0/models"
	"log/slog"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestService_recordData(t *testing.T) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
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

	cacheObj := cache.New()

	srv, err := New(ctx, nsConn, psqlConn, cacheObj)
	if err != nil {
		slog.Error("Creating new service", "Error", err.Error())
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)

	errChan := make(chan error)
	defer close(errChan)

	srvCtx, cancel := context.WithCancel(ctx)

	msgChan := make(chan jetstream.Msg)

	for i := 0; i < 100; i++ {
		if i%3 == 0 {
			fmt.Println(i)
			nsConn.PublishData(struct {
				testData int
			}{testData: i})
		} else {

			nsConn.PublishData(models.Order{
				OrderUid:    strconv.Itoa(i),
				TrackNumber: "WBILMTESTTRACK",
				Entry:       "WBIL",
				Delivery: models.Delivery{
					Name:    "Test Testov",
					Phone:   "+9720000000",
					Zip:     "2639809",
					City:    "Kiryat Mozkin",
					Address: "Ploshad Mira 15",
					Region:  "Kraiot",
					Email:   "test@gmail.com",
				},
				Payment: models.Payment{
					Transaction:  "b563feb7b2b84b6test",
					RequestId:    "",
					Currency:     "USD",
					Provider:     "wbpay",
					Amount:       1817,
					PaymentDt:    1637907727,
					Bank:         "alpha",
					DeliveryCost: 1500,
					GoodsTotal:   317,
					CustomFee:    0,
				},
				Items: []models.Item{{
					ChrtId:      9934930,
					TrackNumber: "WBILMTESTTRACK",
					Price:       453,
					Rid:         "ab4219087a764ae0btest",
					Name:        "Mascaras",
					Sale:        30,
					Size:        "0",
					TotalPrice:  317,
					NmId:        2389212,
					Brand:       "Vivienne Sabo",
					Status:      202,
				}},
				Locale:            "en",
				InternalSignature: "",
				CustomerId:        "test",
				DeliveryService:   "meest",
				Shardkey:          "9",
				SmId:              99,
				DateCreated:       "2021-11-26T06:22:19Z",
				OofShard:          "1",
			})
		}
	}

	go func() {
		for msg := range msgChan {

			wg.Add(1)
			go srv.recordData(srvCtx, msg, &wg)
		}

	}()

	wg.Add(1)
	go nsConn.RunMessageRead(srvCtx, &wg, msgChan, errChan)

	select {
	case <-ctx.Done():
		cancel()
	case x := <-errChan:
		fmt.Println("Я поймал: ", x)
	}
}
