// +build integration_test

package redisstorage_test

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/go-redis/redis/v8"
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
