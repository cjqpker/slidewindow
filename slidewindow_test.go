package slidewindow

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSWOK(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	sw := SlideWindow{
		Concurrency: 4,
		Total:       20,
	}

	initQueue := make(chan uint64, 20)
	taskQueue := make(chan uint64, 20)
	doneQueue := make(chan uint64, 20)

	sw.Init = func(ctx context.Context, s *Session) error {
		initQueue <- s.Index()
		if s.Index() == 20-1 {
			close(initQueue)
		}
		return nil
	}

	sw.Task = func(ctx context.Context, s *Session) error {
		nap := time.Duration(rand.Int()%10) * time.Millisecond
		time.Sleep(nap)
		taskQueue <- s.Index()
		return nil
	}

	sw.Done = func(ctx context.Context, s *Session) error {
		doneQueue <- s.Index()
		if s.Index() == 20-1 {
			close(doneQueue)
			close(taskQueue)
		}
		return nil
	}

	err := sw.Start(context.Background())
	require.NoError(t, err)

	i := 0
	for v := range initQueue {
		require.Equal(t, i, int(v))
		i++
	}

	i = 0
	for v := range doneQueue {
		require.Equal(t, i, int(v))
		i++
	}
}

func TestSWFailedInTask(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	sw := SlideWindow{
		Concurrency: 4,
		Total:       20,
	}

	initQueue := make(chan uint64, 20)
	taskQueue := make(chan uint64, 20)
	doneQueue := make(chan uint64, 20)

	sw.Init = func(ctx context.Context, s *Session) error {
		initQueue <- s.Index()
		if s.Index() == 20-1 {
			close(initQueue)
		}
		return nil
	}

	sw.Task = func(ctx context.Context, s *Session) error {
		if s.Index() == 5 {
			return errors.New("mxy")
		}
		nap := time.Duration(rand.Int()%10) * time.Millisecond
		time.Sleep(nap)
		taskQueue <- s.Index()
		return nil
	}

	sw.Done = func(ctx context.Context, s *Session) error {
		doneQueue <- s.Index()
		if s.Index() == 20-1 {
			close(doneQueue)
			close(taskQueue)
		}
		return nil
	}

	err := sw.Start(context.Background())
	require.Error(t, err)
}

func TestSWFailedInTimeout(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	sw := SlideWindow{
		Concurrency: 4,
		Total:       20,
	}

	initQueue := make(chan uint64, 20)
	taskQueue := make(chan uint64, 20)
	doneQueue := make(chan uint64, 20)

	sw.Init = func(ctx context.Context, s *Session) error {
		initQueue <- s.Index()
		if s.Index() == 20-1 {
			close(initQueue)
		}
		return nil
	}

	sw.Task = func(ctx context.Context, s *Session) error {
		nap := time.Duration(rand.Int()%10+10) * time.Millisecond
		time.Sleep(nap)
		taskQueue <- s.Index()
		return nil
	}

	sw.Done = func(ctx context.Context, s *Session) error {
		doneQueue <- s.Index()
		if s.Index() == 20-1 {
			close(doneQueue)
			close(taskQueue)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	err := sw.Start(ctx)
	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
}
