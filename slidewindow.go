package slidewindow

import (
	"context"
	"sync"
)

type SlideWindow struct {
	Concurrency uint64
	Total       uint64

	Init func(ctx context.Context, s *Session) error // running in order
	Task func(ctx context.Context, s *Session) error // running concurrently
	Done func(ctx context.Context, s *Session) error // running in order
}

type Session struct {
	index  uint64
	values sync.Map
}

func (s *Session) Set(key string, value interface{}) {
	s.values.Store(key, value)
}

func (s *Session) Get(key string) (interface{}, bool) {
	return s.values.Load(key)
}

func (s *Session) Index() uint64 {
	return s.index
}

func (sw *SlideWindow) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var errOccured error

	taskQueue := make(chan *Session, sw.Concurrency)
	finishQueue := make(chan *Session, sw.Concurrency)

	wg := sync.WaitGroup{}

	// start task consumers
	wg.Add(int(sw.Concurrency))
	for i := 0; i < int(sw.Concurrency); i++ {
		go func() {
			defer wg.Done()
			sw.consumeRoutine(ctx, taskQueue, finishQueue, func(err error) {
				cancel()
				errOccured = err
			})
		}()
	}

	// start task producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(taskQueue)
		sw.produceRoutine(ctx, taskQueue, finishQueue, func(err error) {
			cancel()
			errOccured = err
		})
	}()

	wg.Wait()

	return errOccured
}

func (sw *SlideWindow) produceRoutine(ctx context.Context,
	taskQueue chan<- *Session, finishQueue chan *Session, onErr func(err error)) {

	// initialize cells
	cells := make([]Session, sw.Total)
	for i := range cells {
		cells[i].index = uint64(i)
	}

	finishMap := make(map[uint64]*Session, sw.Concurrency)

	produceAt := -1
	finishAt := -1

	// push the first one
	if err := sw.Init(ctx, &cells[0]); err != nil {
		onErr(err)
		return
	}
	taskQueue <- &cells[0]
	produceAt++

	for {
		var task *Session
		var ok bool

		select {
		case <-ctx.Done():
			onErr(ctx.Err())
			return
		case task, ok = <-finishQueue:
			if !ok {
				return
			}
		}

		finishMap[task.index] = task

		// take availables
		for {
			if v, exist := finishMap[uint64(finishAt+1)]; exist {
				if err := sw.Done(ctx, v); err != nil {
					onErr(err)
					return
				}
				delete(finishMap, uint64(finishAt+1))
				finishAt++
			} else {
				break
			}
		}

		// produce
		if produceAt-finishAt < int(sw.Concurrency) {
			for i := 0; i < int(sw.Concurrency)-(produceAt-finishAt); i++ {
				next := produceAt + 1
				if next == int(sw.Total) {
					break
				}
				if err := sw.Init(ctx, &cells[next]); err != nil {
					onErr(err)
					return
				}
				taskQueue <- &cells[next]
				produceAt = next
			}
		}

		if finishAt == int(sw.Total-1) {
			close(finishQueue)
		}
	}
}

func (sw *SlideWindow) consumeRoutine(ctx context.Context,
	taskQueue <-chan *Session, finishQueue chan<- *Session, onErr func(err error)) {
	for {
		var task *Session
		var ok bool
		select {
		case <-ctx.Done():
			onErr(ctx.Err())
			return
		case task, ok = <-taskQueue:
			if !ok {
				return
			}
		}

		// do task
		if err := sw.Task(ctx, task); err != nil {
			onErr(err)
			return
		}

		// notify
		finishQueue <- task
	}
}
