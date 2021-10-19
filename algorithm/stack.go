package algorithm

import "errors"

type Stack struct {
	b []int
	t int //空き領域の先頭のインデックス
}

func NewStack(n int) Stack {
	s := Stack{}
	s.b = make([]int, n)
	s.t = 0
	return s
}

func (s *Stack) IsEmpty() bool {
	return s.t == 0
}

func (s *Stack) Size() int {
	return s.t
}

func (s *Stack) Push(x int) {
	if s.Size()+1 > len(s.b) {
		buff := make([]int, 4*len(s.b))
		copy(buff[:len(s.b)], s.b)
		s.b = buff
	}
	s.b[s.t] = x
	s.t += 1
}

func (s *Stack) Pop() (res int) {
	if s.IsEmpty() {
		panic(errors.New("stack is empty, cannot pop"))
	}
	res = s.b[s.t-1]
	s.t -= 1
	return
}
