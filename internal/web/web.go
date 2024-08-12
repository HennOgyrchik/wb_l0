package web

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
)

func Start(url string) error {
	router := gin.Default()
	router.LoadHTMLGlob("./internal/web/site/*")

	router.GET("/order", Test)
	router.GET("/", home)

	err := router.Run(url)
	if err != nil {
		return fmt.Errorf("Start gin router", err)
	}

	return nil
}

func Test(c *gin.Context) {
	orderUid := c.Request.FormValue("uid")

	if orderUid == "" {
		home(c)
	}

}

func home(c *gin.Context) {
	c.HTML(http.StatusOK, "index.html", gin.H{
		"title": "my friend",
	})
}
