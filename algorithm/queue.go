package algorithm

import (
	"errors"
	"fmt"
)

type Queue struct {
	b []int
	h int // 最初の要素のインデックス
	t int // 空き領域の先頭のインデックス
}

func NewQueue(n int) Queue {
	q := Queue{}
	q.b = make([]int, n)
	q.h = 0
	q.t = 0
	return q
}

func (q *Queue) IsEmpty() bool {
	return q.h == q.t
}

func (q *Queue) Size() int {
	return (q.t - q.h + len(q.b)) % len(q.b)
}

func (q *Queue) Push(x int) {
	if q.Size()+1 >= len(q.b) {
		buff := make([]int, 4*len(q.b))
		copy(buff[:len(q.b)], q.b)
		q.b = buff
	}
	q.b[q.t] = x
	q.t = (q.t + 1) % len(q.b)
}

func (q *Queue) Pop() (res int) {
	if q.IsEmpty() {
		panic(errors.New("queue is empty, cannot pop"))
	}
	res = q.b[q.h]
	q.h = (q.h + 1) % len(q.b)
	return
}

func (q *Queue) Print() {
	for i := 0; i < q.Size(); i++ {
		index := (q.h + i) % len(q.b)
		if i == 0 {
			fmt.Printf("%d", q.b[index])

		} else {

			fmt.Printf(", %d", q.b[index])
		}
	}
}
