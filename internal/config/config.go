package config

import (
	"context"
	"fmt"
	"github.com/sethvargo/go-envconfig"
	"net/url"
	"strconv"
)

type Config struct {
	Postgres PostgresConfig `env:",prefix=PSQL_" json:",omitempty"`
	NS       NSConfig       `env:",prefix=NS_" json:",omitempty"`
	Web      WebConfig      `env:",prefix=WEB_" json:",omitempty"`
}

type PostgresConfig struct {
	Host        string `env:"HOST,default=localhost" json:",omitempty"`
	Port        int    `env:"PORT,default=5432" json:",omitempty"`
	Name        string `env:"NAME,default=my_db" json:",omitempty"`
	User        string `env:"USER,default=my_username" json:",omitempty"`
	Password    string `env:"PASSWORD,default=123" json:",omitempty"`
	SSLMode     string `env:"SSL_MODE,default=disable" json:",omitempty"`
	ConnTimeout int    `env:"CONN_TIMEOUT,default=5" json:",omitempty"`
}

type NSConfig struct {
	Host string `env:"HOST,default=localhost" json:",omitempty"`
	Port int    `env:"PORT,default=4223" json:",omitempty"`
}

type WebConfig struct {
	Host string `env:"HOST,default=localhost" json:",omitempty"`
	Port int    `env:"PORT,default=8080" json:",omitempty"`
}

func (p PostgresConfig) ConnectionURL() (string, error) {
	host := p.Host
	v := p.Port
	if v < 1 && v > 65536 {
		return "", fmt.Errorf("PSQL_PORT invalid")
	}
	host = host + ":" + strconv.Itoa(p.Port)

	u := &url.URL{
		Scheme: "postgres",
		Host:   host,
		Path:   p.Name,
	}

	if p.User == "" || p.Password == "" {
		return "", fmt.Errorf("PSQL_USER or PSQL_PASSWORD invalid")
	}
	u.User = url.UserPassword(p.User, p.Password)

	q := u.Query()
	connTimeout := p.ConnTimeout
	if connTimeout < 1 {
		return "", fmt.Errorf("PSQL_CONN_TIMEOUT invalid")
	}
	q.Add("connect_timeout", strconv.Itoa(p.ConnTimeout))

	if p.SSLMode != "disable" && p.SSLMode != "enable" {
		return "", fmt.Errorf("PSQL_SSL_MODE invalid")
	}
	q.Add("sslmode", p.SSLMode)

	u.RawQuery = q.Encode()

	return u.String(), nil
}

func (ns NSConfig) ConnectionURL() string {
	return fmt.Sprintf("%s:%d", ns.Host, ns.Port)
}

func (w WebConfig) ConnectionURL() string {
	return fmt.Sprintf("%s:%d", w.Host, w.Port)
}

func Read(ctx context.Context) (*Config, error) {
	var cfg Config

	if err := envconfig.Process(ctx, &cfg); err != nil {
		return nil, fmt.Errorf("env processing: %w", err)
	}

	return &cfg, nil

}
