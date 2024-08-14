package cache

import (
	"l0/models"
	"sync"
)

type Cache struct {
	cache map[string]models.Order
}

func New() *Cache {
	return &Cache{cache: make(map[string]models.Order)}
}

func (c *Cache) Fill(rw *sync.RWMutex, orders []models.Order) {
	rw.Lock()
	for _, order := range orders {
		c.cache[order.OrderUid] = order
	}
	rw.Unlock()
}
