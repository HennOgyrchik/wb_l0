package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"l0/internal/cache"
	"l0/internal/ns"
	"l0/internal/psql"
	"l0/models"
	"sync"
)

type Service struct {
	nats  *ns.NS
	psql  *psql.PSQL
	cache *cache.Cache
	rw    *sync.RWMutex
}

func New(ctx context.Context, nats *ns.NS, psql *psql.PSQL, cache *cache.Cache) (*Service, error) {
	var rw sync.RWMutex

	orders, err := psql.GetOrders(ctx)
	if err != nil {
		return nil, fmt.Errorf("Reading DB", err)
	}
	cache.Fill(&rw, orders)

	return &Service{
		nats:  nats,
		psql:  psql,
		cache: cache,
		rw:    &rw,
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	var wg sync.WaitGroup

	msgChan := make(chan jetstream.Msg)
	defer close(msgChan)

	errChan := make(chan error)
	defer close(errChan)

	defer wg.Wait()

	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1) // запуск прослушки NATS Streaming
	go s.nats.GetMessages(serviceCtx, &wg, msgChan, errChan)

	wg.Add(1) // запуск прослушки канала с сообщениями
	go s.msgListener(serviceCtx, &wg, msgChan, errChan)

	//s.nats.PublishTestData()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errChan:
		return err
	}
}

func (s *Service) msgListener(ctx context.Context, wg *sync.WaitGroup, msgChan chan jetstream.Msg, errChan chan error) { // а что делать если не удалось отправить сообщение в бд? вернуть в очередь? добавить обратный канал true/false (возможны последствия)?
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

	s.cache.Fill(s.rw, []models.Order{order})

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
