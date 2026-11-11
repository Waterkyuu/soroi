package comparison_test

import (
	"context"
	"io"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// BenchmarkRedisBurst compares identical cold bursts against a real, private loopback Redis source.
// Redis is the shared application loader for both libraries, not a comparison of distributed-cache adapters.
func BenchmarkRedisBurst(b *testing.B) {
	remote := newBenchmarkRedis(b)
	ctx := context.Background()
	if err := remote.Set(ctx, "benchmark:user", 42, time.Hour).Err(); err != nil {
		b.Fatal(err)
	}
	for _, library := range []string{"Soroi", "Otter"} {
		b.Run(library, func(b *testing.B) {
			benchmarkBurst(b, library, func() uint64 {
				value, err := remote.Get(ctx, "benchmark:user").Uint64()
				if err != nil {
					b.Error(err)
				}
				return value
			})
		})
	}
}

// newBenchmarkRedis owns a disposable server and client; no existing Redis data is accessed.
func newBenchmarkRedis(b *testing.B) *redis.Client {
	b.Helper()
	if _, err := exec.LookPath("redis-server"); err != nil {
		b.Skip("install redis-server to run the real Redis source benchmark")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		b.Fatal(err)
	}
	command := exec.Command("redis-server", "--bind", "127.0.0.1", "--port", strconv.Itoa(port),
		"--save", "", "--appendonly", "no", "--dir", b.TempDir())
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Start(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	remote := redis.NewClient(&redis.Options{
		Addr:       net.JoinHostPort("127.0.0.1", strconv.Itoa(port)),
		MaxRetries: -1, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
	})
	b.Cleanup(func() { _ = remote.Close() })
	ready, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if err := remote.Ping(ready).Err(); err == nil {
			return remote
		}
		select {
		case <-ready.Done():
			b.Fatal("private Redis server did not become ready")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
