package storage

import (
	"errors"
	"time"
)

type Compactor struct {
	engine   *Engine
	ttl      time.Duration
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func NewCompactor(engine *Engine, ttl time.Duration, interval time.Duration) (*Compactor, error) {
	if engine == nil {
		return nil, errors.New("engine required")
	}
	if ttl <= 0 {
		return nil, errors.New("ttl must be positive")
	}
	if interval <= 0 {
		interval = ttl
	}
	return &Compactor{
		engine:   engine,
		ttl:      ttl,
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}, nil
}

func (c *Compactor) Start() {
	ticker := time.NewTicker(c.interval)
	go func() {
		defer close(c.doneCh)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_, _ = c.engine.CompactTombstones(c.ttl, time.Now())
			case <-c.stopCh:
				return
			}
		}
	}()
}

func (c *Compactor) Stop() {
	close(c.stopCh)
	<-c.doneCh
}
