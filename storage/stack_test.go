package storage_test

import (
	"fmt"
	"testing"

	"github.com/tychyDB/storage"
)

func TestStack(t *testing.T) {
	s := storage.NewStack(10)
	s.Push(10)
	s.Push(11)
	res := s.Pop()
	if res != 11 {
		t.Errorf("expected: 11, actual: %d", res)
	}
	res = s.Pop()
	if res != 10 {
		t.Errorf("expected: 10, actual: %d", res)
	}

	s.Push(12)
	s.Push(13)
	s.Push(14)
	s.Push(15)
	s.Push(16)
	s.Push(17)
	s.Push(18)
	s.Push(19)
	s.Push(20)
	for i := 20; i >= 12; i-- {
		res = s.Pop()
		if res != i {
			t.Errorf("expected: %d, actual: %d", i, res)
		}

	}
}

func TestStackExpand(t *testing.T) {
	s := storage.NewStack(16)
	for i := 0; i < 1030; i++ {
		s.Push(i)
	}
	for i := 1029; i >= 0; i-- {
		res := s.Pop()
		if res != i {
			fmt.Printf("%d", res)
			t.Errorf("expected: %d, actual: %d", i, res)
		}
	}

}
