// A minimalistic cache implementation for golang using, hopefully clusterized, redis servers.
package cache

import (
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	. "github.com/beatuslapis/gorelib.v0/connector"

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/util"
)

var (
	ErrNoKey = errors.New("No key")
	ErrNoConnector = errors.New("GoReCache requires Connector")
	ErrNilPointer = errors.New("Nil pointer is not permitted")
	ErrRESPParse = errors.New("RESP parse error")
	ErrSetFailed = errors.New("Set operation failed by constraint")
)

// CacheOptions describes various paramters to control cache behaviors
type CacheOptions struct {

	// Marshaling function to serialize a key or a value for the cache
	Marshal func(interface{}) ([]byte, error)
	
	// Unmarshaling function to deserialize a key or a value for the cache
	Unmarshal func([]byte, interface{}) error
	
	// Cache expiration time
	Expiration time.Duration	
}

// Main object for the cache
type Cache struct {
	connector Connector
	options *CacheOptions
	
	// Counters for statistical usages
	hits, misses, loads int64
}

// NewCache returns a Cache with given connector and options.
// If no options given, i.e. nil, it set them with default values.
func NewCache(connector Connector, options *CacheOptions) (*Cache, error) {
	if connector == nil {
		return nil, ErrNoConnector
	}
	cache := &Cache{
		connector: connector,
		options: options,
	}
	if cache.options == nil {
		cache.options = &CacheOptions{
			defaultMarshal,
			defaultUnmarshal,
			60 * time.Second,
		}
	}
	if cache.options.Marshal == nil {
		cache.options.Marshal = defaultMarshal
	}
	if cache.options.Unmarshal == nil {
		cache.options.Unmarshal = defaultUnmarshal
	}
	return cache, nil
}

// defaultMarshal would be same with json.Marshal,
// except for the string type, or []byte exactly.
// Simply for redis-side manageability and performance.
func defaultMarshal(v interface{}) ([]byte, error) {
	switch vt := v.(type) {
	case []byte:
		return vt, nil
	default:
		return json.Marshal(v)
	}
}

// defaultUnmarshal would be same with json.Unmarshal,
// except for the string type, or []byte exactly.
// Simply for redis-side manageability and performance.
func defaultUnmarshal(d []byte, v interface{}) error {
	switch vt := v.(type) {
	case *[]byte:
		if vt == nil {
			return ErrNilPointer
		}
		*vt = d
		return nil
	default:
		return json.Unmarshal(d, v)
	}
}

// returns timestamp in millis
func getSerial() int64 {
	return time.Now().UnixNano() / 1000
}

// Lua script for getting a cached value.
// Cached values are stored in a size-limited sorted set.
// This script would get the most recent value and check its validity with a given serial
// If valid, returns the value with its serial.
const luaForGet =
	"local cur=redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES') " +
	"if cur[1] and cur[2] and tonumber(cur[2]) > tonumber(ARGV[1]) then " +
	"  return {cur[1], math.floor(cur[2])}" +
	"end " +
	"return false "

// Get returns a cached value using bound Connector.
// It takes key, value parameters as an interface{} type and performs marshal/unmarshal for them.
func (c *Cache) Get(key interface{}, val interface{}) (int64, error) {
	bkey, err := c.options.Marshal(key)
	if err != nil {
		return 0, err
	}
	client, disconnect, validSince, err := c.connector.Connect(bkey)
	if err != nil {
		return 0, err
	}
	defer func(){ if disconnect != nil { disconnect() } }()
	
	resp := util.LuaEval(client, luaForGet, 1, bkey, validSince)
	if resp.Err != nil {
		return 0, resp.Err
	}
	if resp.IsType(redis.Nil) {
		atomic.AddInt64(&c.misses, 1)
		return 0, ErrNoKey
	}

	if resp.IsType(redis.Array) {
		if res, err := resp.Array(); err == nil && len(res) == 2 {
			if res[0].IsType(redis.BulkStr) && res[1].IsType(redis.Int) {
				bval, _ := res[0].Bytes()
				serial, _ := res[1].Int64()
				if err := c.options.Unmarshal(bval, val); err != nil {
					return 0, err
				}
				atomic.AddInt64(&c.hits, 1)
				return serial, nil
			}
		}
	}
	return 0, ErrRESPParse
}

// Lua script for setting a cache value.
// Cached values are stored in a size-limited sorted set.
// This script would make sure a given value is the most recent one.
// Then it adds a value to the set, truncates the set to maintain the size,
// and sets expiration time.
const luaForSet =
	"local cur=redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES') " +
	"if cur[1] and cur[2] and tonumber(cur[2]) > tonumber(ARGV[2]) then " +
	"  return false " +
	"end " +
	"redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1]) " +
	"redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -11) " +
	"if tonumber(ARGV[3]) > 0 then " +
	"  redis.call('EXPIRE', KEYS[1], ARGV[3]) " +
	"end " +
	"return 1"

// Set put a value with a key into the Cache.
// It takes key, value parameters as an interface{} type and performs marshal for them.
// If succeed, it returns a serial number(an unix timestamp in millis) for the value.
// If a value of newer serial already exists, Set would fail with ErrSetFailed.
func (c *Cache) Set(key interface{}, val interface{}) (int64, error) {
	bkey, err := c.options.Marshal(key)
	if err != nil {
		return 0, err
	}
	client, disconnect, _, err := c.connector.Connect(bkey)
	if err != nil {
		return 0, err
	}
	defer func(){ if disconnect != nil { disconnect() } }()
	
	bval, err := c.options.Marshal(val)
	if err != nil {
		return 0, err
	}
	serial := getSerial()
	resp := util.LuaEval(client, luaForSet, 1, bkey, bval, serial, c.options.Expiration.Seconds())
	if resp.Err != nil {
		return 0, resp.Err
	}
	if resp.IsType(redis.Nil) {
		return 0, ErrSetFailed
	}

	atomic.AddInt64(&c.loads, 1)
	return serial, nil
}

// Lua script for setting a cache value.
// Cached values are stored in a size-limited sorted set.
// This script would make sure a given value is the most recent one,
// also the stored value is not newer than given serial.
// Then it adds a value to the set, truncates the set to maintain the size,
// and sets expiration time.
const luaForCheckAndSet =
	"local cur=redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES') " +
	"if cur[1] and cur[2] then " +
	"  if tonumber(cur[2]) > tonumber(ARGV[2]) then " +
	"    return false " +
	"  end " +
	"  if tonumber(cur[2]) > tonumber(ARGV[3]) then " +
	"    return false " +
	"  end " +
	"end " +
	"redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1]) " +
	"redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -11) " +
	"if tonumber(ARGV[4]) > 0 then " +
	"  redis.call('EXPIRE', KEYS[1], ARGV[4]) " +
	"end " +
	"return 1 "

// CheckAndSet put a value with a key into the Cache,
// ONLY IF it has no newer values with, i.e. no update since, a given serial.
// It takes key, value parameters as an interface{} type and performs marshal for them.
// If succeed, it returns a serial number(an unix timestamp in millis) for the value.
// If a value of newer serial already exists, CheckAndSet would fail with ErrSetFailed.
func (c *Cache) CheckAndSet(key interface{}, val interface{}, oserial int64) (int64, error) {
	bkey, err := c.options.Marshal(key)
	if err != nil {
		return 0, err
	}
	client, disconnect, _, err := c.connector.Connect(bkey)
	if err != nil {
		return 0, err
	}
	defer func(){ if disconnect != nil { disconnect() } }()
	
	bval, err := c.options.Marshal(val)
	if err != nil {
		return 0, err
	}
	nserial := getSerial()
	resp := util.LuaEval(client, luaForCheckAndSet, 1, bkey, bval, oserial, nserial, c.options.Expiration.Seconds())
	if resp.Err != nil {
		return 0, resp.Err
	}
	if resp.IsType(redis.Nil) {
		return 0, ErrSetFailed
	}

	atomic.AddInt64(&c.loads, 1)
	return nserial, nil
}

// Del remove a cached value for the given key.
// It takes a key parameter as an interface{} type and performs marshal for it.
func (c *Cache) Del(key interface{}) error {
	bkey, err := c.options.Marshal(key)
	if err != nil {
		return err
	}
	client, disconnect, _, err := c.connector.Connect(bkey)
	if err != nil {
		return err
	}
	defer func(){ if disconnect != nil { disconnect() } }()
	
	resp := client.Cmd("DEL", bkey)
	if resp.Err != nil {
		return resp.Err
	}

	return nil
}

// Hits returns the cache hit counter
func (c *Cache) Hits() int64 {
	return atomic.LoadInt64(&c.hits)
}

// Misses return the cache miss counter
func (c *Cache) Misses() int64 {
	return atomic.LoadInt64(&c.misses)
}

// Loads return the cache load(set) counter
func (c *Cache) Loads() int64 {
	return atomic.LoadInt64(&c.loads)
}
