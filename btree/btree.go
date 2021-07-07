package main

import "fmt"

type Node struct {
	degree   int
	keys     []int
	isLeaf   bool
	children []*Node
}

func newNode(degree int) *Node {
	n := &Node{}
	n.isLeaf = true
	n.degree = degree
	return n
}

func (n *Node) insertInt(idx int, val int) {
	n.keys = append(n.keys, 0)
	copy(n.keys[idx+1:], n.keys[idx:])
	n.keys[idx] = val
}

func (n *Node) insertNode(idx int, item *Node) {
	n.children = append(n.children, nil)
	copy(n.children[idx+1:], n.children[idx:])
	n.children[idx] = item
}

func (n *Node) locateLocally(x int) int {
	for k := range n.keys {
		if x <= n.keys[k] {
			return k
		}
	}
	return len(n.keys)
}

func (n *Node) insertRec(x int) (key int, m *Node) {
	i := n.locateLocally(x)
	if n.isLeaf {
		n.insertInt(i, x)
	} else {
		inserted_key, inserted_m := n.children[i].insertRec(x)
		if inserted_m != nil {
			n.insertInt(i, inserted_key)
			n.insertNode(i, inserted_m)
		}
	}
	if len(n.keys) < n.degree {
		key = 0
		m = nil
	} else {
		mid := len(n.keys) / 2
		key = n.keys[mid]
		m = newNode(n.degree)
		m.isLeaf = n.isLeaf
		m.keys = make([]int, mid)
		copy(m.keys, n.keys[:mid])
		n.keys = n.keys[mid+1:]
		if !n.isLeaf {
			m.children = make([]*Node, mid+1)
			copy(m.children, n.children[:mid+1])
			n.children = n.children[mid+1:]
		}
	}
	return
}

type BTree struct {
	top    *Node
	degree int
}

func NewTree(degree int) *BTree {
	top := newNode(degree)
	t := &BTree{}
	t.top = top
	t.degree = degree
	return t
}

func (t *BTree) Insert(x int) {
	k, m := t.top.insertRec(x)
	if m != nil {
		n := newNode(t.degree)
		n.isLeaf = false
		n.keys = append(n.keys, k)
		n.children = append(n.children, m, t.top)
		t.top = n
	}
}

func (top *Node) Print() {
	if len(top.keys) == 0 {
		fmt.Println("Nothing to Print")
		return
	}

	fmt.Println("------------")
	j := 0
	n := 1
	var queue []*Node
	var height []int
	queue = append(queue, top)
	height = append(height, 1)

	for j < n {
		cur := queue[j]
		h := height[j]
		if j != 0 && h != height[j-1] {
			fmt.Print("\n")
		}
		fmt.Print(cur.keys)
		if !cur.isLeaf {
			queue = append(queue, cur.children...)
			s := make([]int, len(cur.children))
			for i := range cur.children {
				s[i] = h + 1
				n++
			}
			height = append(height, s...)
		}
		j++
	}
	fmt.Print("\n")

	fmt.Println("------------")
}

func main() {
	t := NewTree(3)
	t.Insert(10)
	t.Insert(20)
	t.Insert(30)
	t.Insert(32)
	t.Insert(11)
	t.Insert(9)

	t.top.Print()
	t.Insert(100)

	t.top.Print()
	t.Insert(103)

	t.top.Print()
	t.Insert(108)

	t.top.Print()
	t.Insert(111)

	t.top.Print()
	t.Insert(150)

	t.top.Print()
	t.Insert(80)
	t.top.Print()
}
