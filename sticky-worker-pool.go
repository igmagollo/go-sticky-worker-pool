package gostickyworkerpool

import (
	"context"
	"errors"
	"hash"
	"hash/fnv"
	"sync"
	"time"
)

var (
	ErrStopTimeout       = errors.New("stop timeout")
	ErrWorkerPoolStopped = errors.New("worker pool has stopped")
)

// This worker pool uses a hash function to stick keys with workers in order to
// provide a first-in-first-out behaviour among the workloads with the same key.
// There is no guarantee of ordering between diferent keys.
type StickyWorkerPool struct {
	config      *stickyWorkerPoolConfig
	workFn      WorkFn
	workloadChs []chan workload
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	hasher      hash.Hash32
	stopped     chan struct{}
}

// WorkFn is the function responsible for handling workloads.
// It will receive the context, key and args passed to Send function
// The error will be send to the error channel returned from Send function
type WorkFn func(ctx context.Context, key string, args any) error

// stickyWorkerPoolConfig carry the configuration of the worker pool
type stickyWorkerPoolConfig struct {
	// concurrency measns how many go routines will be running at the same time
	// Default: 10
	concurrency uint

	// channelsBufferSize is the size of each channel
	// Each go routine has its channel to receive workloads and this configuration
	// impacts in how many workloads it can buffer before the channel blocks
	// Default: 10
	channelsBufferSize uint

	// When stopping, the worker pool will stop accepting workloads, and then
	// it will wait until either all buffered workload is finished or it reaches
	// timeout.
	// If zero, timeout is ignored
	// Default: 10 seconds
	stopTimeout time.Duration
}

type stickyWorkerPoolOption func(config *stickyWorkerPoolConfig)

type workload struct {
	ctx   context.Context
	key   string
	args  any
	errCh chan<- error
}

// Creates a new StickyWorkerPool
func NewStickyWorkerPool(workFn WorkFn, options ...stickyWorkerPoolOption) *StickyWorkerPool {
	config := stickyWorkerPoolConfigDefaults()
	for _, option := range options {
		option(config)
	}

	workerPool := &StickyWorkerPool{
		config:  config,
		workFn:  workFn,
		wg:      sync.WaitGroup{},
		hasher:  fnv.New32a(),
		stopped: make(chan struct{}),
	}

	workerPool.workloadChs = make([]chan workload, 0, workerPool.config.concurrency)
	for i := uint(0); i < workerPool.config.concurrency; i++ {
		ch := make(chan workload, workerPool.config.channelsBufferSize)
		workerPool.workloadChs = append(workerPool.workloadChs, ch)
	}

	return workerPool
}

// Start starts the workers of the pool
// It creates starts <concurrency> go routines a.k.a. workers
func (s *StickyWorkerPool) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)

	for i := uint(0); i < s.config.concurrency; i++ {
		go s.worker(ctx, s.workloadChs[i])
	}
}

// This is the worker loop
// It basically runs WorkerFn for each workload sent and redirect the resulting
// error to the error channel of that workload
func (s *StickyWorkerPool) worker(ctx context.Context, workloadsCh <-chan workload) {
	for work := range workloadsCh {
		select {
		case <-ctx.Done():
			work.errCh <- ctx.Err()
			close(work.errCh)
		default:
			err := s.workFn(work.ctx, work.key, work.args)
			work.errCh <- err
			close(work.errCh)
			s.wg.Done()
		}
	}
}

// Stop stops the worker
// It prevents worker to receive new workloads returning error and
// await for the workloads flush or timeout
func (s *StickyWorkerPool) Stop() error {
	close(s.stopped)
	defer s.cancel()
	for _, ch := range s.workloadChs {
		close(ch)
	}

	if s.config.stopTimeout == 0 {
		s.wg.Wait()
		return nil
	}

	c := make(chan struct{})
	go func() {
		defer close(c)
		s.wg.Wait()
	}()
	select {
	case <-c:
		return nil
	case <-time.After(s.config.stopTimeout):
		return ErrStopTimeout
	}
}

func (s *StickyWorkerPool) Send(ctx context.Context, key string, args any) (<-chan error, error) {
	select {
	case <-s.stopped:
		return nil, ErrWorkerPoolStopped
	default:
	}

	errCh := make(chan error, 1)
	s.wg.Add(1)
	s.chanOf(key) <- workload{
		ctx:   ctx,
		key:   key,
		args:  args,
		errCh: errCh,
	}
	return errCh, nil
}

func (s *StickyWorkerPool) hash(key string) uint {
	s.hasher.Reset()
	s.hasher.Write([]byte(key))
	return uint(s.hasher.Sum32())
}

func (s *StickyWorkerPool) chanOf(key string) chan<- workload {
	index := s.hash(key) % uint(len(s.workloadChs))
	return s.workloadChs[index]
}

func stickyWorkerPoolConfigDefaults() *stickyWorkerPoolConfig {
	return &stickyWorkerPoolConfig{
		concurrency:        10,
		channelsBufferSize: 10,
		stopTimeout:        time.Second * 10,
	}
}

// Set Concurrency of the worker pool config
func WithConcurrency(concurrency uint) stickyWorkerPoolOption {
	return func(config *stickyWorkerPoolConfig) {
		config.concurrency = concurrency
	}
}

// Set ChannelsBufferSize of the worker pool config
func WithChannelsBufferSize(channelsBufferSize uint) stickyWorkerPoolOption {
	return func(config *stickyWorkerPoolConfig) {
		config.channelsBufferSize = channelsBufferSize
	}
}

// Set Stop timeout of the worker pool config
func WithStopTimeout(timeout time.Duration) stickyWorkerPoolOption {
	return func(config *stickyWorkerPoolConfig) {
		config.stopTimeout = timeout
	}
}
