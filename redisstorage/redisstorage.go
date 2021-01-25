package redisstorage

import (
	"context"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-tk/versionedkv"
	"github.com/go-tk/versionedkv-redis/redisstorage/internal"
)

// Options represents options for redis storages.
type Options struct {
	Prefix                       string
	NumberOfShards               int
	MaxNumberOfIdleSubscriptions int
	IdleSubscriptionTimeout      time.Duration
}

func (o *Options) sanitize() {
	if o.Prefix == "" {
		o.Prefix = "versionedkv:"
	}
	if o.NumberOfShards < 1 {
		o.NumberOfShards = 10
	}
}

// New creates a new redis storage with the given options.
func New(client redis.UniversalClient, options Options) versionedkv.Storage {
	var rs redisStorage
	rs.client = client
	rs.options = options
	rs.options.sanitize()
	pubSub := client.Subscribe(context.Background())
	eventBusOptions := internal.EventBusOptions{
		ChannelNamePrefix:            rs.options.Prefix + "event:",
		MaxNumberOfIdleSubscriptions: rs.options.MaxNumberOfIdleSubscriptions,
		IdleSubscriptionTimeout:      rs.options.IdleSubscriptionTimeout,
	}
	rs.eventBus.Init(pubSub, eventBusOptions)
	rs.closure = make(chan struct{})
	return &rs
}

type redisStorage struct {
	client   redis.UniversalClient
	options  Options
	eventBus internal.EventBus
	closure  chan struct{}
}

func (rs *redisStorage) GetValue(ctx context.Context, key string) (string, versionedkv.Version, error) {
	value, version, err := rs.doGetValue(ctx, key)
	return value, version2OpaqueVersion(version), err
}

func (rs *redisStorage) doGetValue(ctx context.Context, key string) (string, int64, error) {
	if rs.eventBus.IsClosed() {
		return "", 0, versionedkv.ErrStorageClosed
	}
	shardIndex := rs.locateShard(key)
	hashTag := rs.hashTag(shardIndex)
	pipeline := rs.client.TxPipeline()
	valuesKey := rs.valuesKey(hashTag)
	cmd1 := pipeline.HGet(ctx, valuesKey, key)
	versionsKey := rs.versionsKey(hashTag)
	cmd2 := pipeline.HGet(ctx, versionsKey, key)
	pipeline.Exec(ctx)
	version, err := cmd2.Int64()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		return "", 0, err
	}
	value, err := cmd1.Result()
	if err != nil {
		if err != redis.Nil {
			return "", 0, err
		}
	}
	return value, version, nil
}

func (rs *redisStorage) WaitForValue(ctx context.Context, key string, oldOpaqueVersion versionedkv.Version) (string, versionedkv.Version, error) {
	value, newVersion, err := rs.doWaitForValue(ctx, key, opaqueVersion2Version(oldOpaqueVersion))
	return value, version2OpaqueVersion(newVersion), err
}

func (rs *redisStorage) doWaitForValue(ctx context.Context, key string, oldVersion int64) (string, int64, error) {
	const script = `local valuesKey = KEYS[1]
local versionsKey = KEYS[2]
local key = ARGV[1]
local oldVersion = tonumber(ARGV[2])
local newVersionStr = redis.call("HGET", versionsKey, key)
local newVersion = newVersionStr == false and 0 or tonumber(newVersionStr)
if newVersion == oldVersion then
	return {"", 0, 0}
end
if newVersionStr == false then
	return {"", 0, 1}
end
local value = redis.call("HGET", valuesKey, key)
if value == false then
	value = ""
end
return {value, newVersion, 1}`
	shardIndex := rs.locateShard(key)
	hashTag := rs.hashTag(shardIndex)
	valuesKey := rs.valuesKey(hashTag)
	versionsKey := rs.versionsKey(hashTag)
	scriptKeys := []string{
		valuesKey,
		versionsKey,
	}
	scriptArgv := []interface{}{
		key,
		oldVersion,
	}
	for {
		var retry bool
		value, newVersion, err := func() (string, int64, error) {
			watcher, err := rs.eventBus.AddWatcher(key)
			if err != nil {
				if err == internal.ErrEventBusClosed {
					err = versionedkv.ErrStorageClosed
				}
				return "", 0, err
			}
			defer func() {
				if watcher != (internal.Watcher{}) {
					rs.eventBus.RemoveWatcher(key, watcher)
				}
			}()
			result, err := rs.client.Eval(ctx, script, scriptKeys, scriptArgv...).Result()
			if err != nil {
				return "", 0, err
			}
			results := result.([]interface{})
			ok := results[2].(int64) == 1
			retry = !ok
			if retry {
				select {
				case <-watcher.Event():
					watcher = internal.Watcher{}
					return "", 0, nil
				case <-rs.closure:
					watcher = internal.Watcher{}
					return "", 0, versionedkv.ErrStorageClosed
				case <-ctx.Done():
					return "", 0, ctx.Err()
				}
			}
			value := results[0].(string)
			newVersion := results[1].(int64)
			return value, newVersion, nil
		}()
		if err != nil {
			return "", 0, err
		}
		if retry {
			continue
		}
		return value, newVersion, nil
	}
}

func (rs *redisStorage) CreateValue(ctx context.Context, key string, value string) (versionedkv.Version, error) {
	version, err := rs.doCreateValue(ctx, key, value)
	return version2OpaqueVersion(version), err
}

func (rs *redisStorage) doCreateValue(ctx context.Context, key string, value string) (int64, error) {
	if rs.eventBus.IsClosed() {
		return 0, versionedkv.ErrStorageClosed
	}
	const script = `local valuesKey = KEYS[1]
local versionsKey = KEYS[2]
local versionHighKey = KEYS[3]
local key = ARGV[1]
local value = ARGV[2]
local shardIndex = tonumber(ARGV[3])
local numberOfShards = tonumber(ARGV[4])
local channelNamePrefix = ARGV[5]
if redis.call("HEXISTS", versionsKey, key) == 1 then
	return 0
end
redis.call("HSET", valuesKey, key, value)
local version = redis.call("INCR", versionHighKey) * numberOfShards + shardIndex
redis.call("HSET", versionsKey, key, tostring(version))
redis.call("PUBLISH", channelNamePrefix .. key, "")
return version`
	shardIndex := rs.locateShard(key)
	hashTag := rs.hashTag(shardIndex)
	valuesKey := rs.valuesKey(hashTag)
	versionsKey := rs.versionsKey(hashTag)
	versionHighKey := rs.versionHighKey(hashTag)
	scriptKeys := []string{
		valuesKey,
		versionsKey,
		versionHighKey,
	}
	scriptArgv := []interface{}{
		key,
		value,
		shardIndex,
		rs.options.NumberOfShards,
		rs.eventBus.ChannelNamePrefix(),
	}
	result, err := rs.client.Eval(ctx, script, scriptKeys, scriptArgv...).Result()
	if err != nil {
		return 0, err
	}
	version := result.(int64)
	return version, nil
}

func (rs *redisStorage) UpdateValue(ctx context.Context, key, value string, oldOpaqueVersion versionedkv.Version) (versionedkv.Version, error) {
	newVersion, err := rs.doUpdateValue(ctx, key, value, opaqueVersion2Version(oldOpaqueVersion))
	return version2OpaqueVersion(newVersion), err
}

func (rs *redisStorage) doUpdateValue(ctx context.Context, key, value string, oldVersion int64) (int64, error) {
	if rs.eventBus.IsClosed() {
		return 0, versionedkv.ErrStorageClosed
	}
	const script = `local valuesKey = KEYS[1]
local versionsKey = KEYS[2]
local versionHighKey = KEYS[3]
local key = ARGV[1]
local value = ARGV[2]
local oldVersion = tonumber(ARGV[3])
local shardIndex = tonumber(ARGV[4])
local numberOfShards = tonumber(ARGV[5])
local channelNamePrefix = ARGV[6]
local versionStr = redis.call("HGET", versionsKey, key)
if versionStr == false then
	return 0
end
if oldVersion ~= 0 and tonumber(versionStr) ~= oldVersion then
	return 0
end
redis.call("HSET", valuesKey, key, value)
local newVersion = redis.call("INCR", versionHighKey) * numberOfShards + shardIndex
redis.call("HSET", versionsKey, key, tostring(newVersion))
redis.call("PUBLISH", channelNamePrefix .. key, "")
return newVersion`
	shardIndex := rs.locateShard(key)
	hashTag := rs.hashTag(shardIndex)
	valuesKey := rs.valuesKey(hashTag)
	versionsKey := rs.versionsKey(hashTag)
	versionHighKey := rs.versionHighKey(hashTag)
	scriptKeys := []string{
		valuesKey,
		versionsKey,
		versionHighKey,
	}
	scriptArgv := []interface{}{
		key,
		value,
		oldVersion,
		shardIndex,
		rs.options.NumberOfShards,
		rs.eventBus.ChannelNamePrefix(),
	}
	result, err := rs.client.Eval(ctx, script, scriptKeys, scriptArgv...).Result()
	if err != nil {
		return 0, err
	}
	newVersion := result.(int64)
	return newVersion, nil
}

func (rs *redisStorage) CreateOrUpdateValue(ctx context.Context, key, value string, oldOpaqueVersion versionedkv.Version) (versionedkv.Version, error) {
	newVersion, err := rs.doCreateOrUpdateValue(ctx, key, value, opaqueVersion2Version(oldOpaqueVersion))
	return version2OpaqueVersion(newVersion), err
}

func (rs *redisStorage) doCreateOrUpdateValue(ctx context.Context, key, value string, oldVersion int64) (int64, error) {
	if rs.eventBus.IsClosed() {
		return 0, versionedkv.ErrStorageClosed
	}
	const script = `local valuesKey = KEYS[1]
local versionsKey = KEYS[2]
local versionHighKey = KEYS[3]
local key = ARGV[1]
local value = ARGV[2]
local oldVersion = tonumber(ARGV[3])
local shardIndex = tonumber(ARGV[4])
local numberOfShards = tonumber(ARGV[5])
local channelNamePrefix = ARGV[6]
local versionStr = redis.call("HGET", versionsKey, key)
if versionStr == false then
	redis.call("HSET", valuesKey, key, value)
	local version = redis.call("INCR", versionHighKey) * numberOfShards + shardIndex
	redis.call("HSET", versionsKey, key, tostring(version))
	redis.call("PUBLISH", channelNamePrefix .. key, "")
	return version
end
if oldVersion ~= 0 and tonumber(versionStr) ~= oldVersion then
	return 0
end
redis.call("HSET", valuesKey, key, value)
local newVersion = redis.call("INCR", versionHighKey) * numberOfShards + shardIndex
redis.call("HSET", versionsKey, key, tostring(newVersion))
redis.call("PUBLISH", channelNamePrefix .. key, "")
return newVersion`
	shardIndex := rs.locateShard(key)
	hashTag := rs.hashTag(shardIndex)
	valuesKey := rs.valuesKey(hashTag)
	versionsKey := rs.versionsKey(hashTag)
	versionHighKey := rs.versionHighKey(hashTag)
	scriptKeys := []string{
		valuesKey,
		versionsKey,
		versionHighKey,
	}
	scriptArgv := []interface{}{
		key,
		value,
		oldVersion,
		shardIndex,
		rs.options.NumberOfShards,
		rs.eventBus.ChannelNamePrefix(),
	}
	result, err := rs.client.Eval(ctx, script, scriptKeys, scriptArgv...).Result()
	if err != nil {
		return 0, err
	}
	newVersion := result.(int64)
	return newVersion, nil
}

func (rs *redisStorage) DeleteValue(ctx context.Context, key string, opaqueVersion versionedkv.Version) (bool, error) {
	return rs.doDeleteValue(ctx, key, opaqueVersion2Version(opaqueVersion))
}

func (rs *redisStorage) doDeleteValue(ctx context.Context, key string, version int64) (bool, error) {
	if rs.eventBus.IsClosed() {
		return false, versionedkv.ErrStorageClosed
	}
	const script = `local valuesKey = KEYS[1]
local versionsKey = KEYS[2]
local key = ARGV[1]
local version = tonumber(ARGV[2])
local channelNamePrefix = ARGV[3]
local versionStr = redis.call("HGET", versionsKey, key)
if versionStr == false then
	return 0
end
if version ~= 0 and tonumber(versionStr) ~= version then
	return 0
end
redis.call("HDEL", valuesKey, key)
redis.call("HDEL", versionsKey, key)
redis.call("PUBLISH", channelNamePrefix .. key, "")
return 1`
	shardIndex := rs.locateShard(key)
	hashTag := rs.hashTag(shardIndex)
	valuesKey := rs.valuesKey(hashTag)
	versionsKey := rs.versionsKey(hashTag)
	scriptKeys := []string{
		valuesKey,
		versionsKey,
	}
	scriptArgv := []interface{}{
		key,
		version,
		rs.eventBus.ChannelNamePrefix(),
	}
	result, err := rs.client.Eval(ctx, script, scriptKeys, scriptArgv...).Result()
	if err != nil {
		return false, err
	}
	ok := result.(int64) == 1
	return ok, nil
}

func (rs *redisStorage) Close() error {
	err := rs.eventBus.Close()
	if err == internal.ErrEventBusClosed {
		return versionedkv.ErrStorageClosed
	}
	close(rs.closure)
	return err
}

func (rs *redisStorage) Inspect(ctx context.Context) (versionedkv.StorageDetails, error) {
	if rs.eventBus.IsClosed() {
		return versionedkv.StorageDetails{IsClosed: true}, nil
	}
	var valueDetails map[string]versionedkv.ValueDetails
	for shardIndex := 0; shardIndex < rs.options.NumberOfShards; shardIndex++ {
		hashTag := rs.hashTag(shardIndex)
		valuesKey := rs.valuesKey(hashTag)
		cmd := rs.client.HGetAll(ctx, valuesKey)
		key2Value, err := cmd.Result()
		if err != nil {
			return versionedkv.StorageDetails{}, err
		}
		for key, value := range key2Value {
			if valueDetails == nil {
				valueDetails = make(map[string]versionedkv.ValueDetails)
			}
			temp := valueDetails[key]
			temp.V = value
			valueDetails[key] = temp
		}
		versionsKey := rs.versionsKey(hashTag)
		cmd = rs.client.HGetAll(ctx, versionsKey)
		key2VersionStr, err := cmd.Result()
		if err != nil {
			return versionedkv.StorageDetails{}, err
		}
		for key, versionStr := range key2VersionStr {
			if valueDetails == nil {
				valueDetails = make(map[string]versionedkv.ValueDetails)
			}
			version, err := strconv.ParseInt(versionStr, 10, 64)
			if err != nil {
				return versionedkv.StorageDetails{}, err
			}
			temp := valueDetails[key]
			temp.Version = version
			valueDetails[key] = temp
		}
	}
	return versionedkv.StorageDetails{
		Values: valueDetails,
	}, nil
}

func (rs *redisStorage) locateShard(key string) int {
	hash := fnv.New64a()
	hash.Write([]byte(key))
	shardIndex := int(hash.Sum64() % uint64(rs.options.NumberOfShards))
	return shardIndex
}

func (rs *redisStorage) hashTag(shardIndex int) string {
	buffer := make([]byte, 0, 16)
	buffer = strconv.AppendInt(buffer, int64(shardIndex), 10)
	buffer = append(buffer, '_')
	buffer = strconv.AppendInt(buffer, int64(rs.options.NumberOfShards), 10)
	return string(buffer)
}

func (rs *redisStorage) valuesKey(hashTag string) string {
	return rs.options.Prefix + "values:{" + hashTag + "}"
}

func (rs *redisStorage) versionsKey(hashTag string) string {
	return rs.options.Prefix + "versions:{" + hashTag + "}"
}

func (rs *redisStorage) versionHighKey(hashTag string) string {
	return rs.options.Prefix + "versionhigh:{" + hashTag + "}"
}

func version2OpaqueVersion(version int64) versionedkv.Version {
	if version == 0 {
		return nil
	}
	return version
}

func opaqueVersion2Version(opaqueVersion versionedkv.Version) int64 {
	if opaqueVersion == nil {
		return 0
	}
	return opaqueVersion.(int64)
}
