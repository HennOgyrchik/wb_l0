package psql

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"l0/models"
	"log/slog"
	"time"
)

type PSQL struct {
	db      *pgxpool.Pool
	timeout time.Duration
}

func New(ctx context.Context, connURL string, timeout time.Duration) (*PSQL, error) {
	pool, err := pgxpool.Connect(ctx, connURL)
	if err != nil {
		return nil, err
	}
	return &PSQL{db: pool, timeout: timeout}, nil
}

func (p *PSQL) InsertOne(ctx context.Context, msg models.Order) (err error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	//_, err = p.db.Exec(ctx, "insert into funds (tag,balance) values ($1,$2)")
	slog.Debug("Отправка в БД", "Данные:", msg)
	fmt.Println("Отправка в БД", msg)
	return
}
