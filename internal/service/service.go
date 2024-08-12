package service

import (
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go/jetstream"
	"l0/internal/ns"
	"l0/internal/psql"
	"l0/models"
	"sync"
)

type Service struct {
	nats *ns.NS
	psql *psql.PSQL
}

func New(nats *ns.NS, psql *psql.PSQL) *Service {
	return &Service{
		nats: nats,
		psql: psql,
	}
}

func (s *Service) Start(ctx context.Context) error {
	var wg sync.WaitGroup

	msgChan := make(chan jetstream.Msg)
	var err error

	wg.Add(1)
	go func() {
		if err = s.nats.GetMessages(msgChan); err != nil {
			close(msgChan)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			msg, ok := <-msgChan
			if !ok {
				break
			}
			var order models.Order

			err = json.Unmarshal(msg.Data(), &order)
			if err != nil {
				close(msgChan)
				return
			}
			if err = s.psql.InsertOne(ctx, order); err != nil {
				close(msgChan)
				return
			}
		}
	}()

	wg.Wait()
	return err
}

func (s *Service) Stop() {
	s.nats.PublishTestData()
}
