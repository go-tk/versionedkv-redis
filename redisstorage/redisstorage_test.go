package redisstorage_test

import (
	"testing"

	"github.com/go-tk/versionedkv"
)

func TestRedisStorage(t *testing.T) {
	versionedkv.DoTestStorage(t, func() (versionedkv.Storage, error) {
		return makeStorage()
	})
}
