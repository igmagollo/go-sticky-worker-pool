package gostickyworkerpool

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestNewStickyWorkerPool(t *testing.T) {
	t.Parallel()
	for _, tt := range newStickyWorkerPoolTable {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Act
			got := NewStickyWorkerPool(tt.workFn, tt.options...)

			// Assert
			if got.config.concurrency != tt.expectedConcurrency {
				t.Errorf("config.concurrency: expected %d, got %d", tt.expectedConcurrency, got.config.concurrency)
			}
			if got.config.channelsBufferSize != tt.expectedChannelsBufferSize {
				t.Errorf("config.channelsBufferSize: expected %d, got %d", tt.expectedChannelsBufferSize, got.config.channelsBufferSize)
			}
			if got.config.stopTimeout != tt.expectedStopTimeout {
				t.Errorf("config.stopTimeout: expected %d, got %d", tt.expectedConcurrency, got.config.concurrency)
			}
			if got.workFn == nil {
				t.Errorf("workFn: expected not nil, got nil")
			}
			if uint(len(got.workloadChs)) != tt.expectedConcurrency {
				t.Errorf("len(workloadChs): expected %d, got %d", tt.expectedConcurrency, len(got.workloadChs))
			}
			for i, ch := range got.workloadChs {
				if cap(ch) != int(tt.expectedChannelsBufferSize) {
					t.Errorf("cap(workloadChs[%d]): expected %d, got %d", i, tt.expectedChannelsBufferSize, cap(ch))
				}
			}

		})
	}
}

var newStickyWorkerPoolTable = []struct {
	name    string
	workFn  WorkFn
	options []stickyWorkerPoolOption

	expectedConcurrency        uint
	expectedChannelsBufferSize uint
	expectedStopTimeout        time.Duration
}{
	{
		name:    "should create a work pool with defaults",
		workFn:  func(ctx context.Context, key string, args any) error { return nil },
		options: []stickyWorkerPoolOption{},

		expectedConcurrency:        10,
		expectedChannelsBufferSize: 10,
		expectedStopTimeout:        10 * time.Second,
	},
	{
		name:    "should create a work pool with 20 concurrency",
		workFn:  func(ctx context.Context, key string, args any) error { return nil },
		options: []stickyWorkerPoolOption{WithConcurrency(20)},

		expectedConcurrency:        20,
		expectedChannelsBufferSize: 10,
		expectedStopTimeout:        10 * time.Second,
	},
	{
		name:    "should create a work pool with 20 ChannelsBufferSize",
		workFn:  func(ctx context.Context, key string, args any) error { return nil },
		options: []stickyWorkerPoolOption{WithChannelsBufferSize(20)},

		expectedConcurrency:        10,
		expectedChannelsBufferSize: 20,
		expectedStopTimeout:        10 * time.Second,
	},
	{
		name:    "should create a work pool with 20 seconds of timeout",
		workFn:  func(ctx context.Context, key string, args any) error { return nil },
		options: []stickyWorkerPoolOption{WithStopTimeout(20 * time.Second)},

		expectedConcurrency:        10,
		expectedChannelsBufferSize: 10,
		expectedStopTimeout:        20 * time.Second,
	},
}

func TestStickyWorkerPool_Start(t *testing.T) {
	t.Parallel()
	for _, tt := range StickyWorkerPool_StartTable {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			sut := NewStickyWorkerPool(tt.workFn)

			// Act
			sut.Start(context.Background())
			sut.wg.Add(int(sut.config.concurrency))
			errChs := make([]chan error, 0, sut.config.concurrency)
			for i := uint(0); i < sut.config.concurrency; i++ {
				errCh := make(chan error, 1)
				sut.workloadChs[i] <- workload{
					ctx:   context.Background(),
					key:   "",
					args:  i,
					errCh: errCh,
				}
				errChs = append(errChs, errCh)
			}

			// Assert
			for _, errCh := range errChs {
				select {
				case <-errCh:
				case <-time.After(time.Millisecond):
				}
			}
		})
	}
}

var StickyWorkerPool_StartTable = []struct {
	name    string
	workFn  WorkFn
	options []stickyWorkerPoolOption
}{
	{
		name:    "should start workers for default concurrency",
		workFn:  func(ctx context.Context, key string, args any) error { return nil },
		options: []stickyWorkerPoolOption{},
	},
	{
		name:    "should start workers for 20 concurrency",
		workFn:  func(ctx context.Context, key string, args any) error { return nil },
		options: []stickyWorkerPoolOption{WithConcurrency(20)},
	},
}

func TestStickyWorkerPool_Stop(t *testing.T) {
	t.Parallel()
	for _, tt := range StickyWorkerPool_StopTable {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			sut := NewStickyWorkerPool(tt.workFn)

			// Act
			sut.Start(context.Background())
			sut.wg.Add(int(sut.config.concurrency))
			errChs := make([]chan error, 0, sut.config.concurrency)
			for i := uint(0); i < sut.config.concurrency; i++ {
				errCh := make(chan error, 1)
				sut.workloadChs[i] <- workload{
					ctx:   context.Background(),
					key:   "",
					args:  i,
					errCh: errCh,
				}
				errChs = append(errChs, errCh)
			}
			err := sut.Stop()

			// Assert
			if err != nil {
				t.Errorf("expect error to not occur, got %s", err.Error())
			}

			for _, errCh := range errChs {
				select {
				case <-errCh:
				default:
					t.Errorf("expect channel to receive nil, got blocked channel")
				}
			}
		})
	}

	t.Run("should return timout err", func(t *testing.T) {
		t.Parallel()
		// Arrange
		sut := NewStickyWorkerPool(func(ctx context.Context, key string, args any) error {
			time.Sleep(1 * time.Second)
			return nil
		}, WithStopTimeout(1*time.Nanosecond))

		// Act
		sut.wg.Add(int(sut.config.concurrency))
		for i := uint(0); i < sut.config.concurrency; i++ {
			sut.workloadChs[i] <- workload{
				ctx:   context.Background(),
				key:   "",
				args:  i,
				errCh: make(chan error, 1),
			}
		}
		sut.Start(context.Background())
		err := sut.Stop()

		// Assert
		if !errors.Is(err, ErrStopTimeout) {
			t.Errorf("expect error to be %s, got %s", ErrStopTimeout.Error(), err.Error())
		}
	})
}

var StickyWorkerPool_StopTable = []struct {
	name    string
	workFn  WorkFn
	options []stickyWorkerPoolOption
}{
	{
		name: "should start workers for default concurrency",
		workFn: func(ctx context.Context, key string, args any) error {
			return nil
		},
		options: []stickyWorkerPoolOption{},
	},
	{
		name: "should start workers for 20 concurrency",
		workFn: func(ctx context.Context, key string, args any) error {
			return nil
		},
		options: []stickyWorkerPoolOption{WithConcurrency(20)},
	},
	{
		name: "should start workers for 100 concurrency",
		workFn: func(ctx context.Context, key string, args any) error {
			return nil
		},
		options: []stickyWorkerPoolOption{WithConcurrency(100)},
	},
}

func TestStickyWorkerPool_Send(t *testing.T) {
	t.Parallel()
	t.Run("should return timout err", func(t *testing.T) {
		t.Parallel()
		// Arrange
		entries := map[string]int{
			"1":  0,
			"2":  0,
			"3":  0,
			"4":  0,
			"5":  0,
			"6":  0,
			"7":  0,
			"8":  0,
			"9":  0,
			"10": 0,
		}
		mutex := sync.RWMutex{}
		sut := NewStickyWorkerPool(func(ctx context.Context, key string, args any) error {
			// simulate some IO
			time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))

			newVal := args.(int)
			mutex.RLock()
			curr := entries[key]
			mutex.RUnlock()
			if newVal != curr+1 {
				t.Errorf("entry[%s]: expect %d, got %d", key, curr+1, newVal)
			}
			mutex.Lock()
			entries[key] = newVal
			mutex.Unlock()

			return nil
		})
		sut.Start(context.Background())

		// Act
		for i := 0; i < 10000; i++ {
			key := strconv.Itoa(i % 10)
			args := i/10 + 1

			_, err := sut.Send(context.Background(), key, args)
			if err != nil {
				t.Errorf("expect nil error, got '%s'", err.Error())
			}
		}

		// Assert
		sut.Stop()
	})
}
