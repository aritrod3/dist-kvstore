package storage

import (
	"strconv"
	"sync"
	"testing"
)

func TestConcurrentPutSameKey(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(Config{DataDir: dir, Shards: 8})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	const writes = 100
	var wg sync.WaitGroup
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = engine.Put("key", []byte(strconv.Itoa(i)))
		}(i)
	}
	wg.Wait()

	entry, ok := engine.GetEntry("key")
	if !ok {
		t.Fatalf("expected key")
	}
	if entry.Version != writes {
		t.Fatalf("expected version %d, got %d", writes, entry.Version)
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(Config{DataDir: dir, Shards: 8})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key-" + strconv.Itoa(i%5)
			_, _ = engine.Put(key, []byte("value"))
		}(i)
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key-" + strconv.Itoa(i%5)
			_, _ = engine.Get(key)
		}(i)
	}
	wg.Wait()
}
