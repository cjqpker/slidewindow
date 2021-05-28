package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cjqpker/slidewindow"
)

func main() {
	sw := slidewindow.SlideWindow{
		Total:       10,
		Concurrency: 4,
	}

	// Init(s) run in order
	sw.Init = func(ctx context.Context, s *slidewindow.Session) error {
		s.Set("value", time.Now().UnixNano())
		return nil
	}

	// Task(s) run concurrently
	sw.Task = func(ctx context.Context, s *slidewindow.Session) error {
		value, exist := s.Get("value")
		if !exist {
			return errors.New("should never happen")
		}

		s.Set("value", value.(int64)+10)
		return nil
	}

	// Done(s) run in order
	resultQueue := make(chan int64, 10)
	sw.Done = func(ctx context.Context, s *slidewindow.Session) error {
		value, exist := s.Get("value")
		if !exist {
			return errors.New("should never happen")
		}
		resultQueue <- value.(int64)

		if s.Index() == 10-1 {
			close(resultQueue)
		}
		return nil
	}

	// start running
	if err := sw.Start(context.TODO()); err != nil {
		panic(err)
	}

	// check result
	var last int64 = 0
	for v := range resultQueue {
		if v < last {
			panic("should never happen")
		}
		last = v
	}

	fmt.Println("done")
}
