package main

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// stoppedRedis starts and kills its own disposable server, retaining an open client for real transport errors.
func stoppedRedis(ctx context.Context) (*redis.Client, func(), error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return nil, nil, err
	}
	command := exec.CommandContext(ctx, "redis-server", "--bind", "127.0.0.1", "--port", strconv.Itoa(port),
		"--save", "", "--appendonly", "no")
	if err := command.Start(); err != nil {
		return nil, nil, fmt.Errorf("start disposable redis-server: %w", err)
	}
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { _ = command.Process.Kill(); _ = command.Wait() }) }
	remote := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), ContextTimeoutEnabled: true,
		MaxRetries: -1, DialerRetries: 1, DialTimeout: 10 * time.Millisecond, ReadTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Millisecond,
	})
	cleanup := func() { _ = remote.Close(); stop() }
	ready, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		if err := remote.Ping(ready).Err(); err == nil {
			break
		}
		select {
		case <-ready.Done():
			cleanup()
			return nil, nil, fmt.Errorf("wait for disposable Redis: %w", ready.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	stop()
	return remote, cleanup, nil
}
