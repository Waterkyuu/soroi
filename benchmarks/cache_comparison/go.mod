module github.com/Waterkyuu/soroi/benchmarks/cache_comparison

go 1.26.0

require (
	github.com/Waterkyuu/soroi v0.3.2
	github.com/maypok86/otter/v2 v2.4.0
	github.com/redis/go-redis/v9 v9.23.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/Waterkyuu/soroi => ../..
