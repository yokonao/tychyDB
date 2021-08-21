package bptree

import (
	"errors"
	"fmt"
)

const MaxDegree = 3
const Infity = (1 << 32) - 1 // uint(4byte) max value

type Record struct {
	value int
}

type Node struct {
	isLeaf   bool
	keys     []int
	pointers []interface{}
}

func newNode() *Node {
	node := &Node{}
	return node
}

func (n *Node) locateLocally(key int) int {
	// keys 1, 3, 5, 7
	// search key: 2
	// return 1 because 1 <= 2 < 3

	// search key: 3
	// return 3 because 3 <= 3 < 5

	for k := range n.keys {
		if key < n.keys[k] {
			return k
		}
	}
	return len(n.keys)
}

func (n *Node) insertRec(key int, value int) (splitted bool, splitKey int, leftNode *Node) {
	insert_idx := n.locateLocally(key)

	if n.isLeaf {
		record := &Record{value: value}
		// keysにinsert recordにinsert
		if len(n.keys) == 0 {
			n.keys = append(n.keys, key)
			n.pointers = append(n.pointers, record)
		} else {

			n.keys = append(n.keys, 0)
			copy(n.keys[insert_idx+1:], n.keys[insert_idx:])
			n.keys[insert_idx] = key

			n.pointers = append(n.pointers, nil)
			copy(n.pointers[insert_idx+1:], n.pointers[insert_idx:])
			n.pointers[insert_idx] = record
		}
	} else {
		target := n.pointers[insert_idx].(*Node)
		splitted, splitKey, leftNode := target.insertRec(key, value)
		if splitted {
			n.keys = append(n.keys, 0)
			copy(n.keys[insert_idx+1:], n.keys[insert_idx:])
			n.keys[insert_idx] = splitKey

			n.pointers = append(n.pointers, nil)
			copy(n.pointers[insert_idx+1:], n.pointers[insert_idx:])
			n.pointers[insert_idx] = leftNode
		}
	}

	// 分割
	if n.isLeaf && len(n.keys) >= MaxDegree {
		splitted = true
		splitIndex := len(n.keys) / 2
		splitKey = n.keys[splitIndex]
		// nodeの分割
		leftNode = newNode()
		leftNode.isLeaf = n.isLeaf
		leftNode.keys = make([]int, splitIndex)
		leftNode.pointers = make([]interface{}, splitIndex)
		copy(leftNode.keys, n.keys[:splitIndex])
		copy(leftNode.pointers, n.pointers[:splitIndex])
		n.keys = n.keys[splitIndex:]
		n.pointers = n.pointers[splitIndex:]
		return
	} else if !n.isLeaf && len(n.keys) > MaxDegree {
		splitted = true
		splitIndex := (len(n.keys) - 1) / 2
		splitKey = n.keys[splitIndex]
		// nodeの分割
		leftNode = newNode()
		leftNode.isLeaf = n.isLeaf
		leftNode.keys = make([]int, splitIndex)
		leftNode.pointers = make([]interface{}, splitIndex)
		copy(leftNode.keys, n.keys[:splitIndex])
		copy(leftNode.pointers, n.pointers[:splitIndex])
		leftNode.keys = append(leftNode.keys, Infity)
		leftNode.pointers = append(leftNode.pointers, n.pointers[splitIndex])

		n.keys = n.keys[splitIndex+1:]
		n.pointers = n.pointers[splitIndex+1:]
		return
	} else {
		splitted = false
		return
	}
}

type BPTree struct {
	top *Node
}

func NewBPTree() *BPTree {
	t := &BPTree{}
	return t
}

func (t *BPTree) Insert(key int, value int) {
	if key >= Infity {
		panic(errors.New("key too large"))
	}
	if t.top == nil {
		top := newNode()

		top.isLeaf = false
		top.keys = append(top.keys, Infity)

		child := newNode()
		child.isLeaf = true
		child.insertRec(key, value)

		top.pointers = append(top.pointers, child)
		t.top = top
		return
	}

	splitted, splitKey, leftNode := t.top.insertRec(key, value)
	if splitted {
		top := newNode()
		top.isLeaf = false
		top.keys = append(top.keys, splitKey)
		top.keys = append(top.keys, Infity)
		top.pointers = append(top.pointers, leftNode)
		top.pointers = append(top.pointers, t.top)
		t.top = top
	}
}

func (t *BPTree) Print() {
	top := t.top
	if len(top.keys) == 0 {
		fmt.Println("Nothing to Print")
		return
	}
	fmt.Printf("infinity is set to %d\n", Infity)

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
			for k := range cur.pointers {
				queue = append(queue, cur.pointers[k].(*Node))
			}
			s := make([]int, len(cur.pointers))
			for i := range cur.pointers {
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
