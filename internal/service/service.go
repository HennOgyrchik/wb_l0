package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"l0/internal/ns"
	"l0/internal/psql"
	"l0/models"
	"sync"
)

type Service struct {
	nats  *ns.NS
	psql  *psql.PSQL
	cache map[string]models.Order
	rw    *sync.RWMutex
}

func New(ctx context.Context, nats *ns.NS, psql *psql.PSQL) (*Service, error) {
	var rw sync.RWMutex
	cache := make(map[string]models.Order)

	orders, err := psql.GetOrders(ctx)
	if err != nil {
		return nil, fmt.Errorf("Reading DB", err)
	}

	rw.Lock()
	for _, order := range orders {
		cache[order.OrderUid] = order
	}
	rw.Unlock()

	return &Service{
		nats:  nats,
		psql:  psql,
		cache: cache,
		rw:    &rw,
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	var err error
	msgChan := make(chan jetstream.Msg)

	wg.Add(1) // запуск прослушки NATS Streaming
	go func() {
		if err = s.nats.GetMessages(msgChan); err != nil {
			close(msgChan)
		}
		wg.Done()
	}()

	wg.Add(1) // запуск прослушки канала с сообщениями
	go func() {
		err = s.msgListener(ctx, &wg, msgChan)
		if err != nil {
			close(msgChan)
		}
	}()

	wg.Add(1) //запуск веб-сервера

	//s.nats.PublishTestData()

	wg.Wait()
	return err
}

func (s *Service) msgListener(ctx context.Context, wg *sync.WaitGroup, msgChan chan jetstream.Msg) error { // а что делать если не удалось отправить сообщение в бд? вернуть в очередь?
	defer wg.Done()
	var err error
	for {
		if err != nil {
			break
		}
		msg, ok := <-msgChan
		if !ok {
			break
		}

		go func() {
			err = s.recordData(ctx, msg)
			if err != nil {
				err = fmt.Errorf("Recording data", err)
				return
			}
		}()

	}
	return err
}

func (s *Service) recordData(ctx context.Context, msg jetstream.Msg) error {
	var order models.Order

	err := json.Unmarshal(msg.Data(), &order)
	if err != nil {
		return fmt.Errorf("Unmarshalling data", err)
	}

	s.rw.Lock()
	s.cache[order.OrderUid] = order
	s.rw.Unlock()

	if err = s.psql.InsertOne(ctx, order.OrderUid, msg.Data()); err != nil {
		return fmt.Errorf("Inserting into DB", err)
	}
	return nil
}

func (s *Service) Stop() error {
	err := s.nats.Close()
	if err != nil {
		return err
	}
	s.psql.Close()
	return nil
}
