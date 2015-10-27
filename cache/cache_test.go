package cache

import (
	"fmt"
	"testing"
	"time"
	
	"github.com/beatuslapis/gorecache.v0/connector"
)

type Key struct {
	Key string
	Seq int
}

type Value struct {
	Value string
	Serial int64
}

func getStored(c *Cache, key interface{}, val interface {}) (interface {}, int64, error) {
	switch val.(type) {
	case string:
		var stored string
		if serial, err := c.Get(key, &stored); err != nil {
			return nil, serial, err
		} else {
			return stored, serial, nil
		}
	case Value:
		var stored Value
		if serial, err := c.Get(key, &stored); err != nil {
			return nil, serial, err
		} else {
			return stored, serial, nil
		}
	}
	return nil, 0, nil
}

func testSetAndGet(t *testing.T, key interface{}, val interface{}) {
	connector, err := connector.NewSingle(":6379", 1)
	if err != nil {
		t.Fatal("can't create connector")
	}
	cache, err := NewCache(connector, nil)
	if err != nil {
		t.Fatal("can't create cache")
	}
	
	if serial, err := cache.Set(key, val); err == nil {
		fmt.Println("stored for key:{", key, "} with value:{", val, ":", serial, "}")
	} else {
		t.Fatal("cache.Set failed", err)
	}
	if loads := cache.Loads(); loads != 1 {
		fmt.Println("incorrect load counter", loads)
		t.Fail()
	}

	if stored, serial, err := getStored(cache, key, val); err == nil {
		fmt.Println("stored:{", stored, "} with serial:{", serial, "}")
		if stored != val {
			fmt.Println("assert failed. Got:{", stored, "} expected:{", val, "}")
			t.Fail()
		}
	} else {
		t.Fatal("cache.Get failed", err)
	}
	if hits := cache.Hits(); hits != 1 {
		fmt.Println("incorrect hit counter", hits)
		t.Fail()
	}

	if err := cache.Del(key); err == nil {
		fmt.Println("deleting for key:{", key, "}")
	} else {
		t.Fatal("cache.Del failed", err)
	}

	if stored, serial, err := getStored(cache, key, val); err != nil {
		fmt.Println("confirmed deletion:", err)
		if err != ErrNoKey {
			fmt.Println("unexpected error:", err)
			t.Fail()
		}
	} else {
		t.Fatal("Deleted value exists. {", stored, "} with serial:{", serial, "}")
	}
	if misses := cache.Misses(); misses != 1 {
		fmt.Println("incorrect miss counter", misses)
		t.Fail()
	}
}

func TestString(t *testing.T) {
	key := "basicTest"
	val := "basicTestValue:" + time.Now().String()
	
	testSetAndGet(t, key, val)
}

func TestObject(t *testing.T) {
	key := Key{"okey", 999}
	val := Value{ Value: "oval", Serial: time.Now().Unix()}
	
	testSetAndGet(t, key, val)
}

func TestExpiration(t *testing.T) {
	key := "expirationTest"
	val := "expirationValue:" + time.Now().String()
	
	connector, err := connector.NewSingle(":6379", 1)
	if err != nil {
		t.Fatal("can't create connector")
	}
	cache, err := NewCache(connector, &CacheOptions{ Expiration: 1 * time.Second })
	if err != nil {
		t.Fatal("can't create cache")
	}
		
	if serial, err := cache.Set(key, val); err == nil {
		fmt.Println("stored for key:{", key, "} with value:{", val, ":", serial, "}")
	} else {
		t.Fatal("cache.Set failed", err)
	}
	if loads := cache.Loads(); loads != 1 {
		fmt.Println("incorrect load counter", loads)
		t.Fail()
	}
	
	time.Sleep(2 * time.Second)
	if stored, serial, err := getStored(cache, key, val); err != nil {
		fmt.Println("confirmed expiration:", err)
		if err != ErrNoKey {
			fmt.Println("unexpected error:", err)
			t.Fail()
		}
	} else {
		t.Fatal("Expired value exists. {", stored, "} with serial:{", serial, "}")
	}
	if misses := cache.Misses(); misses != 1 {
		fmt.Println("incorrect miss counter", misses)
		t.Fail()
	}	
}

func TestCheckAndSet(t *testing.T) {
	key := "checkAndSetTest"
	val := "checkAndSetValue:" + time.Now().String()
	
	connector, err := connector.NewSingle(":6379", 1)
	if err != nil {
		t.Fatal("can't create connector")
	}
	cache, err := NewCache(connector, nil)
	if err != nil {
		t.Fatal("can't create cache")
	}
		
	var sserial int64
	if serial, err := cache.Set(key, val); err == nil {
		fmt.Println("stored for key:{", key, "} with value:{", val, ":", serial, "}")
		sserial = serial
	} else {
		t.Fatal("cache.Set failed", err)
	}
	if loads := cache.Loads(); loads != 1 {
		fmt.Println("incorrect load counter", loads)
		t.Fail()
	}
	
	// interleaving update
	var iserial int64
	if serial, err := cache.Set(key, val); err == nil {
		fmt.Println("updated for key:{", key, "} with value:{", val, ":", serial, "}")
		iserial = serial
	} else {
		t.Fatal("cache.Set failed", err)
	}
	if loads := cache.Loads(); loads != 2 {
		fmt.Println("incorrect load counter", loads)
		t.Fail()
	}

	// try checkAndSet
	if _, err := cache.CheckAndSet(key, val, sserial); err == nil {
		fmt.Println("unexpected success of checkAndSet")
		t.Fail()
	}
	if loads := cache.Loads(); loads != 2 {
		fmt.Println("incorrect load counter", loads)
		t.Fail()
	}

	if stored, serial, err := getStored(cache, key, val); err == nil {
		fmt.Println("stored:{", stored, "} with serial:{", serial, "}")
		if iserial != serial {
			fmt.Println("assert failed. Got:{", serial, "} expected:{", iserial, "}")
			t.Fail()
		}
	} else {
		t.Fatal("cache.Get failed", err)
	}
	if hits := cache.Hits(); hits != 1 {
		fmt.Println("incorrect hit counter", hits)
		t.Fail()
	}
		
	if err := cache.Del(key); err == nil {
		fmt.Println("deleting for key:{", key, "}")
	} else {
		t.Fatal("cache.Del failed", err)
	}

	if stored, serial, err := getStored(cache, key, val); err != nil {
		fmt.Println("confirmed deletion:", err)
		if err != ErrNoKey {
			fmt.Println("unexpected error:", err)
			t.Fail()
		}
	} else {
		t.Fatal("Deleted value exists. {", stored, "} with serial:{", serial, "}")
	}	
	if misses := cache.Misses(); misses != 1 {
		fmt.Println("incorrect miss counter", misses)
		t.Fail()
	}	
}