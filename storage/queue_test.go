package storage_test

import (
	"fmt"
	"testing"

	"github.com/tychyDB/storage"
)

func TestQueue(t *testing.T) {
	q := storage.NewQueue(10)
	q.Push(10)
	q.Push(11)
	res := q.Pop()
	if res != 10 {
		t.Errorf("expected: 10, actual: %d", res)
	}

	res = q.Pop()
	if res != 11 {
		t.Errorf("expected: 11, actual: %d", res)
	}

	q.Push(12)
	q.Push(13)
	q.Push(14)
	q.Push(15)
	q.Push(16)
	q.Push(17)
	q.Push(18)
	q.Push(19)
	q.Push(20)
	for i := 12; i <= 20; i++ {
		res = q.Pop()
		if res != i {
			t.Errorf("expected: %d, actual: %d", i, res)
		}

	}
	// これでq.hが一周する
	q.Push(12)
	q.Push(13)
	q.Push(14)
	q.Push(15)
	q.Push(16)
	q.Push(17)
	q.Push(18)
	q.Push(19)
	q.Push(20)
	for i := 12; i <= 20; i++ {
		res = q.Pop()
		if res != i {
			t.Errorf("expected: %d, actual: %d", i, res)
		}
	}

}

func TestQueueExpand(t *testing.T) {
	q := storage.NewQueue(16)
	for i := 0; i < 1030; i++ {
		q.Push(i)
	}
	for i := 0; i < 1030; i++ {
		res := q.Pop()
		if res != i {
			fmt.Printf("%d", res)
			t.Errorf("expected: %d, actual: %d", i, res)
		}
	}

}
