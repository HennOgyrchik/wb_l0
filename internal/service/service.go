package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go/jetstream"
	"l0/internal/cache"
	"l0/internal/ns"
	"l0/internal/psql"
	"l0/models"
	"log/slog"
	"net/http"
	"sync"
)

type Service struct {
	nats  *ns.NS
	psql  *psql.PSQL
	cache *cache.Cache
}

func New(ctx context.Context, nats *ns.NS, psql *psql.PSQL, cache *cache.Cache) (*Service, error) {
	orders, err := psql.GetOrders(ctx)
	if err != nil {
		return nil, fmt.Errorf("Reading DB: %w", err)
	}
	cache.Fill(orders)

	return &Service{
		nats:  nats,
		psql:  psql,
		cache: cache,
	}, nil
}

func (s *Service) Start(ctx context.Context, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()
	var srvWG sync.WaitGroup

	msgChan := make(chan jetstream.Msg)

	defer srvWG.Wait()

	natsCtx, natsCtxCancel := context.WithCancel(ctx)
	defer natsCtxCancel()
	srvWG.Add(1)
	go s.nats.RunMessageRead(natsCtx, &srvWG, msgChan, errChan)

	msgCtx, msgCtxCancel := context.WithCancel(ctx)
	defer msgCtxCancel()
	srvWG.Add(1)
	go s.msgChanListener(msgCtx, &srvWG, msgChan, errChan)

	<-ctx.Done()
}

func (s *Service) msgChanListener(ctx context.Context, wg *sync.WaitGroup, msgChan chan jetstream.Msg, errChan chan error) { // а что делать если не удалось отправить сообщение в бд? вернуть в очередь? добавить обратный канал true/false (возможны последствия)?
	defer wg.Done()

	var listenerWG sync.WaitGroup
	defer listenerWG.Wait()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgChan:
			if !ok {
				errChan <- fmt.Errorf("Message Listener: Channel is closed")
				return
			}
			listenerWG.Add(1)
			go s.recordData(ctx, msg, &listenerWG)
		}
	}
}

func (s *Service) recordData(ctx context.Context, msg jetstream.Msg, wg *sync.WaitGroup) {
	defer wg.Done()
	var order models.Order

	dec := json.NewDecoder(bytes.NewReader(msg.Data()))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&order); err != nil {
		slog.Info("Invalid JSON:", "JSON", msg.Data())
		return
	}

	if err := s.psql.InsertOne(ctx, order.OrderUid, msg.Data()); err != nil {
		slog.Info("Invalid data:", "data", msg.Data())
		return
	}

	s.cache.Fill([]models.Order{order})
}

func (s *Service) Stop() error {
	err := s.nats.Close()
	if err != nil {
		return err
	}
	s.psql.Close()
	return nil
}

func (s *Service) GetOrderInfoHandler(c *gin.Context) {
	orderUid := c.Request.FormValue("uid")

	order, ok := s.cache.GetOrderByUID(orderUid)

	var result gin.H
	if ok {
		result = gin.H{
			"result": order,
		}
	}

	c.HTML(http.StatusOK, "index.html", result)
}

func (s *Service) PublishData(c *gin.Context) {
	data := make(map[string]interface{})

	err := c.BindJSON(&data)
	if err != nil {
		slog.Info("Binding JSON", "Error", err.Error())
		c.JSON(http.StatusBadRequest, struct {
			Error string
		}{err.Error()})
		return
	}

	err = s.nats.PublishData(data)
	if err != nil {
		slog.Info("Publish data", "Error", err.Error())
		c.JSON(http.StatusBadRequest, struct {
			Error string
		}{err.Error()})
		return
	}

}
