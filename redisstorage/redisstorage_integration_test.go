// +build integration_test

package redisstorage_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-tk/backoff"
	"github.com/go-tk/versionedkv"
	. "github.com/go-tk/versionedkv-redis/redisstorage"
)

type storage struct {
	versionedkv.Storage

	c *redis.ClusterClient
}

func makeStorage() (storage, error) {
	var as []string
	for _, rn := range strings.Split(os.Getenv("REDIS_NODES"), " ") {
		as = append(as, rn+":6379")
	}
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: as})
	b := backoff.New(backoff.Options{
		MinDelay:            100 * time.Millisecond,
		MaxDelay:            3 * time.Second,
		MaxNumberOfAttempts: 10,
	})
	for {
		css, err := c.ClusterSlots(context.Background()).Result()
		if err != nil {
			if err2 := b.Do(); err2 != nil {
				c.Close()
				return storage{}, err
			}
			continue
		}
		var n int
		for _, cs := range css {
			n += len(cs.Nodes)
		}
		if n < len(as) {
			if err := b.Do(); err != nil {
				c.Close()
				return storage{}, err
			}
			continue
		}
		break
	}
	sid := int(atomic.AddInt32(&lastStorageID, 1))
	s := New(c, Options{
		Prefix: "versionedkv" + strconv.Itoa(sid),
	})
	return storage{
		Storage: s,

		c: c,
	}, nil
}

func (s storage) Close() error {
	err := s.Storage.Close()
	s.c.Close()
	return err
}

var lastStorageID int32
