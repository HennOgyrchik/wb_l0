package ns

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"l0/models"
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
	//defer nc.Drain()

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

	//defer stream.DeleteConsumer(ctx, "processor-1")

	//msgs, _ := cons.Fetch(1)
	//for msg := range msgs.Messages() {
	//	fmt.Printf("data: %s\n", msg.Data())
	//	msg.Ack()
	//}
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

func (ns *NS) PublishTestData() {
	order1 := models.Order{
		OrderUid:    "b563feb7b2b84b6test",
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
	}
	order1byte, err := json.Marshal(order1)
	if err != nil {
		fmt.Println("Order1JSON:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ns.js.Publish(ctx, "events.orders.b563feb7b2b84b6test", order1byte)
	fmt.Println("Отправил в очередь")

}

func (ns *NS) GetMessages(ctx context.Context, wg *sync.WaitGroup, msgChan chan jetstream.Msg, errChan chan error) {
	defer wg.Done()

	iter, err := ns.consumer.Messages()
	if err != nil {
		errChan <- fmt.Errorf("NATS Streaming Messages: %w", err)
		return
	}

	for {
		msg, err := iter.Next()
		if err != nil {
			errChan <- fmt.Errorf("NATS Streaming Next message: %w", err)
			return
		}

		select {
		case <-ctx.Done():
			return
		case msgChan <- msg:
			if err := msg.Ack(); err != nil {
				errChan <- fmt.Errorf("NATS Streaming Ack: %w", err)
				return
			}
		default:
			errChan <- fmt.Errorf("The channel is unavailable")
			return
		}

	}
}
