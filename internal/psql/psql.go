package psql

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"l0/models"
	"time"
)

type PSQL struct {
	db      *pgxpool.Pool
	timeout time.Duration
}

func New(ctx context.Context, connURL string, timeout time.Duration) (*PSQL, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	pool, err := pgxpool.Connect(ctxTimeout, connURL)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("Creating connect", err)
	}
	cancel()

	err = createTable(ctx, pool, timeout)
	if err != nil {
		pool.Close()
		return nil, err
	}

	return &PSQL{db: pool, timeout: timeout}, nil
}

func (p *PSQL) InsertOne(ctx context.Context, orderUid string, data []byte) (err error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	_, err = p.db.Exec(ctx, "insert into orders (order_id,data) values ($1,$2)", orderUid, data)
	if err != nil {
		err = fmt.Errorf("Insert one", err)
	}
	return err
}

func (p *PSQL) Close() {
	p.db.Close()
}

func (p *PSQL) GetOrders(ctx context.Context) ([]models.Order, error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	rows, err := p.db.Query(ctx, "select data from orders")
	if err != nil {
		return nil, fmt.Errorf("Getting orders", err)
	}

	var orders []models.Order
	for rows.Next() {
		var order models.Order
		if err = rows.Scan(&order); err != nil {
			return nil, fmt.Errorf("Scanning row", err)
		}
		orders = append(orders, order)
	}

	return orders, nil
}

func createTable(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if _, err := pool.Exec(ctx, "create table if not exists orders(order_id varchar(20) unique not null,data json)"); err != nil {
		return fmt.Errorf("Creating table", err)
	}

	return nil
}
