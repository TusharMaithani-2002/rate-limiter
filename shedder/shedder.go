package shedder

import (
	"errors"
	"sync/atomic"
)

var ErrOverload = errors.New("Server is overloaded; request shed")

// Stats provides a snapshot of the shedder's real-time state.
type Stats struct {
	MaxCapacity int64
	InFlight    int64
	// TotalShed counts the number of shed requests due to overload.
	TotalShed int64
}

type ConcurrencyShedder struct {
	sem       chan struct{}
	maxLimit  int64
	inFlight  atomic.Int64
	totalShed atomic.Int64
}

func NewConcurrencyShedder(maxLimit int) *ConcurrencyShedder {
	if maxLimit <= 0 {
		maxLimit = 100
	}
	return &ConcurrencyShedder{
		sem:      make(chan struct{}, maxLimit),
		maxLimit: int64(maxLimit),
	}
}

// TryAcquire attempts to reserve a slot non-blockingly.
// Returns a release function if successful, or ErrOverloaded if full.
func (s *ConcurrencyShedder) Acquire() (func(), error) {
	select {
	case s.sem <- struct{}{}: // Acquired a slot
		s.inFlight.Add(1)

		var once atomic.Bool
		release := func() {
			// Ensure release can only be executed once per acquisition
			if once.CompareAndSwap(false, true) {
				<-s.sem
				s.inFlight.Add(-1)
			}
		}

		return release, nil
	default:
		// Capacity reached: shed immediately without blocking the goroutine
		s.totalShed.Add(1)
		return nil, ErrOverload
	}
}

func (s *ConcurrencyShedder) Stats() Stats {
	return Stats{
		MaxCapacity: s.maxLimit,
		InFlight:    s.inFlight.Load(),
		TotalShed:   s.totalShed.Load(),
	}
}
