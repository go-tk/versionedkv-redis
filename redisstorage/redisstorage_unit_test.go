// +build !integration_test

package redisstorage_test

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/go-tk/versionedkv"
	. "github.com/go-tk/versionedkv-redis/redisstorage"
)

type storage struct {
	versionedkv.Storage

	m *miniredis.Miniredis
	c *redis.Client
}

func makeStorage() (storage, error) {
	m, err := miniredis.Run()
	if err != nil {
		return storage{}, err
	}
	c := redis.NewClient(&redis.Options{Addr: m.Addr()})
	s := New(c, Options{})
	return storage{
		Storage: s,

		m: m,
		c: c,
	}, nil
}

func (s storage) Close() error {
	err := s.Storage.Close()
	s.c.Close()
	s.m.Close()
	return err
}
