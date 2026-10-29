package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun verifies rejection and local reuse against an unavailable Redis client without a server.
func TestRun(t *testing.T) {
	var output bytes.Buffer
	if err := run(context.Background(), &output); err != nil {
		t.Fatal(err)
	}
	want := "local hit: cached\nsecond rejected: true\nfirst: loaded\nsource calls: 1\nrejected: 1\nshared read errors: 1\n"
	if output.String() != want {
		t.Fatalf("output = %q, want %q", output.String(), want)
	}
}
