package web

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"l0/internal/service"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type Web struct {
	connURL string
}

func New(url string) Web {
	return Web{connURL: url}
}
func (w *Web) Start(ctx context.Context, srv *service.Service, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()

	router := gin.Default()
	gin.SetMode(gin.ReleaseMode)

	router.LoadHTMLGlob("./internal/web/site/*")

	router.GET("/order", srv.GetOrderInfoHandler)
	router.PUT("/publish", srv.PublishData)
	router.GET("/", indexPage)

	server := &http.Server{Addr: w.connURL, Handler: router.Handler()}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- fmt.Errorf("Start web server: %w", err)
			return
		}
	}()

	<-ctx.Done()

	ctxSrv, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctxSrv); err != nil {
		slog.Error("Web server was shutdown incorrectly", "Error", err.Error())
		return
	}
}

func indexPage(c *gin.Context) {
	c.HTML(http.StatusOK, "index.html", gin.H{
		"title": "my friend",
	})
}
