package autoscaling

import (
	"sync"
	"time"
)

// ThroughputManager keeps track of the number of tasks processed per bucketDuration over a specified bucketCount.
type ThroughputManager struct {
	mu             sync.Mutex
	counts         []int
	durations      []time.Duration
	pos            int
	bucketCount    int
	bucketDuration time.Duration
	in             chan struct{}
	out            chan struct{}
}

// NewThroughputManager creates a new ThroughputManager with the specified interval.
func NewThroughputManager(bucketCount int, bucketDuration time.Duration) *ThroughputManager {
	return &ThroughputManager{
		counts:         make([]int, bucketCount),
		durations:      make([]time.Duration, bucketCount),
		bucketCount:    bucketCount,
		bucketDuration: bucketDuration,
		in:             make(chan struct{}),
		out:            make(chan struct{}),
	}
}

// Add increments the count of tasks processed in the current bucket
func (m *ThroughputManager) Add(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// increment the current position's value
	m.counts[m.pos] += n
}

// AvgThroughput returns the average number of tasks processed per
// bucketDuration over the past bucketCount buckets.
func (m *ThroughputManager) AvgThroughput() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	sum := 0
	for _, n := range m.counts {
		sum += n
	}
	return float64(sum) / float64(m.bucketCount)
}

// shift shifts the position in the circular buffer every bucketDuration, resetting the old value.
func (m *ThroughputManager) shift() {
	tick := time.NewTicker(m.bucketDuration)
	for {
		select {
		case <-tick.C:
			m.mu.Lock()

			m.pos = (m.pos + 1) % m.bucketCount
			m.counts[m.pos] = 0

			m.mu.Unlock()
		case <-m.in:
			close(m.out)
			return
		}
	}
}

// Start starts the ThroughputManager
// It ticks every bucketDuration, shifting the position in the circular buffer.
func (m *ThroughputManager) Start() {
	go m.shift()
}

func (m *ThroughputManager) Stop() {
	close(m.in)
	<-m.out
}
