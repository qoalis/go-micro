package micro

import (
	"context"
	"github.com/allegro/bigcache/v3"
	"github.com/qoalis/go-micro/util/h"
	log "github.com/sirupsen/logrus"
	"time"
)

type Cache interface {
	Get(target interface{}, key string, populate func() (interface{}, error)) error
}

type DefaultCache struct {
	Cache
	internal *bigcache.BigCache
}

func NewCache(ttl time.Duration) Cache {
	cache, _ := bigcache.New(context.Background(), bigcache.DefaultConfig(ttl))
	return DefaultCache{
		internal: cache,
	}
}

func (c DefaultCache) Get(target interface{}, key string, populate func() (interface{}, error)) error {
	if !h.IsPointer(target) {
		log.Fatal("target must be a pointer")
		return nil
	}
	if value, err := c.internal.Get(key); err == nil {
		return h.DeserializeJsonBytes(value, target)
	} else {
		data, err := populate()
		if err == nil {
			if serialized, err := h.ToJsonBytes(data); err == nil {
				_ = c.internal.Set(key, serialized)
			}
			return h.CopyAllFields(target, data, false)
		}
		return err
	}
}
