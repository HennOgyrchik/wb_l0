package cache

import (
	"l0/models"
	"sync"
)

type Cache struct {
	rw    sync.RWMutex
	cache map[string]models.Order
}

func New() *Cache {
	return &Cache{cache: make(map[string]models.Order)}
}

func (c *Cache) Fill(orders []models.Order) {
	c.rw.Lock()
	for _, order := range orders {
		c.cache[order.OrderUid] = order
	}
	c.rw.Unlock()
}

func (c *Cache) GetOrderByUID(uid string) (models.Order, bool) {
	c.rw.RLock()
	order, ok := c.cache[uid]
	c.rw.RUnlock()

	return order, ok
}
