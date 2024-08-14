package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go/jetstream"
	"l0/internal/cache"
	"l0/internal/ns"
	"l0/internal/psql"
	"l0/models"
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
		return nil, fmt.Errorf("Reading DB", err)
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
	defer close(msgChan)

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
			go s.recordData(ctx, msg, errChan, &listenerWG)

		}
	}

}

func (s *Service) recordData(ctx context.Context, msg jetstream.Msg, errChan chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	var order models.Order

	err := json.Unmarshal(msg.Data(), &order)
	if err != nil {
		errChan <- fmt.Errorf("Unmarshalling data: %w", err)
		return
	}

	s.cache.Fill([]models.Order{order})

	if err = s.psql.InsertOne(ctx, order.OrderUid, msg.Data()); err != nil {
		errChan <- fmt.Errorf("Inserting into DB", err)
	}

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
