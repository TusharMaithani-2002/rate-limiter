package shedder

import (
	"sync"
	"testing"
	"time"
)

func TestConcurrencyShedder_EnforcesCap(t *testing.T) {
	maxConcurrent := 3
	cs := NewConcurrencyShedder(maxConcurrent)

	var releases []func()

	for i := range maxConcurrent {
		release, err := cs.Acquire()
		if err != nil {
			t.Fatalf("expected acquisition %d to succeed, got %v", i+1, err)
		}
		releases = append(releases, release)
	}

	// Next acquisition should fail due to overload
	_, err := cs.Acquire()
	if err != ErrOverload {
		t.Fatalf("expected ErrOverloaded, got %v", err)
	}

	stats := cs.Stats()
	if stats.InFlight != 3 || stats.TotalShed != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	releases[0]() // release slot 0
	if cs.Stats().InFlight != 2 {
		t.Fatalf("expected in-flight to drop to 2, got %d", cs.Stats().InFlight)
	}

	// Should now be able to acquire again
	rel, err := cs.Acquire()
	if err != nil {
		t.Fatalf("expected acquisition after release to succeed, got %v", err)
	}
	rel()

	// Clean up remaining
	for _, r := range releases[1:] {
		r()
	}
}

func TestConcurrencyShedder_EnforceRace(t *testing.T) {
	cs := NewConcurrencyShedder(10)
	var wg sync.WaitGroup
	workers := 50

	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()

			for range 100 {
				release, err := cs.Acquire()
				if err == nil {
					// Simulate some work
					time.Sleep(100 * time.Microsecond)
					release()
				}
			}
		}()
	}

	wg.Wait()
}
