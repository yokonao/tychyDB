package storage

import "errors"

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
	if q.Size() >= len(q.b) {
		panic(errors.New("queue is full, cannot push"))
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
