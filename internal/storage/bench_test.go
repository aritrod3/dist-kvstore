package storage

import (
	"strconv"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	dir := b.TempDir()
	engine, err := Open(Config{DataDir: dir, Shards: 16})
	if err != nil {
		b.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.Put("key-"+strconv.Itoa(i), []byte("value"))
	}
}

func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	engine, err := Open(Config{DataDir: dir, Shards: 16})
	if err != nil {
		b.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	for i := 0; i < 10000; i++ {
		_, _ = engine.Put("key-"+strconv.Itoa(i), []byte("value"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.Get("key-" + strconv.Itoa(i%10000))
	}
}
