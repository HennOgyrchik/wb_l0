package ns

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"sync"
	"time"
)

type NS struct {
	connection *nats.Conn
	stream     jetstream.Stream
	consumer   jetstream.Consumer
	js         jetstream.JetStream
}

func New(ctx context.Context, url string) (*NS, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	js, _ := jetstream.New(nc)

	cfg := jetstream.StreamConfig{
		Name:      "EVENTS",
		Retention: jetstream.WorkQueuePolicy,
		Subjects:  []string{"events.>"},
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)

	stream, err := js.CreateStream(ctxTimeout, cfg)
	if err != nil {
		cancel()
		return nil, err
	}
	cancel()

	ctxTimeout, cancel = context.WithTimeout(ctx, 10*time.Second)
	cons, err := stream.CreateOrUpdateConsumer(ctxTimeout, jetstream.ConsumerConfig{
		Name:          "processor-1",
		FilterSubject: "events.orders.>",
	})
	if err != nil {
		cancel()
		return nil, err
	}
	cancel()

	return &NS{
		connection: nc,
		stream:     stream,
		consumer:   cons,
		js:         js,
	}, nil
}

func (ns *NS) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := ns.stream.DeleteConsumer(ctx, "processor-1")
	if err != nil {
		return err
	}
	ns.connection.Close()
	return nil

}

func (ns *NS) PublishData(data any) error {
	orderByte, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("JSON Marshal:", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = ns.js.Publish(ctx, "events.orders.b563feb7b2b84b6test", orderByte)
	return err

}

func (ns *NS) RunMessageRead(ctx context.Context, wg *sync.WaitGroup, msgChan chan jetstream.Msg, errChan chan error) {
	defer wg.Done()

	iter, err := ns.consumer.Messages()
	if err != nil {
		errChan <- fmt.Errorf("NATS Streaming Messages: %w", err)
		return
	}

	go messageRead(iter, msgChan, errChan)

	<-ctx.Done()
	iter.Stop()

}

func messageRead(iter jetstream.MessagesContext, msgChan chan jetstream.Msg, errChan chan error) {
	defer close(msgChan)
	for {
		msg, err := iter.Next()
		switch {
		case errors.Is(err, jetstream.ErrMsgIteratorClosed):
			return
		case err != nil:
			errChan <- fmt.Errorf("NATS Streaming Next message: %w", err)
			return
		}

		msgChan <- msg

		if err := msg.Ack(); err != nil {
			errChan <- fmt.Errorf("NATS Streaming Ack: %w", err)
			return
		}
	}
}
