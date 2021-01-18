package redisstorage_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/go-tk/versionedkv"
	. "github.com/go-tk/versionedkv-redis/redisstorage"
	"github.com/stretchr/testify/assert"
)

func TestRedisStorage(t *testing.T) {
	versionedkv.DoTestStorage(t, func() (versionedkv.Storage, error) {
		return makeStorage()
	})
}

func TestRedisStorage_Close(t *testing.T) {
	s, err := makeStorage()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	time.AfterFunc(1*time.Second, func() {
		s.Close() // WaitForValue should fail with error ErrStorageClosed
	})
	_, _, err = s.WaitForValue(context.Background(), "foo", nil)
	assert.Equal(t, err, versionedkv.ErrStorageClosed)
}

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
