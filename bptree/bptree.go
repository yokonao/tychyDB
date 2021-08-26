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

func newNodeLeaf(len int) *Node {
	node := &Node{}
	node.isLeaf = true
	node.keys = make([]int, len)
	node.pointers = make([]interface{}, len)
	return node
}

func newNodeNonLeaf(len int) *Node {
	node := &Node{}
	node.isLeaf = false
	node.keys = make([]int, len)
	node.pointers = make([]interface{}, len)
	return node
}

func (n *Node) locateLocally(key int) int {
	for k := range n.keys {
		if key < n.keys[k] {
			return k
		}
	}
	return len(n.keys)
}

func (n *Node) findRec(key int) (find bool, value int) {
	if n.isLeaf {
		// binary search
		l := 0
		r := len(n.keys)
		for r-l > 1 {
			mid := (l + r) / 2
			if n.keys[mid] <= key {
				l = mid
			} else {
				r = mid
			}
		}
		if n.keys[l] == key {
			return true, n.pointers[l].(*Record).value
		}
		find = false
		return
	} else {
		index := n.locateLocally(key)
		return n.pointers[index].(*Node).findRec(key)
	}
}

func insertInt(index int, item int, arr []int) []int {
	arr = append(arr, 0)
	copy(arr[index+1:], arr[index:])
	arr[index] = item
	return arr
}

func insertPtr(index int, ptr interface{}, arr []interface{}) []interface{} {
	arr = append(arr, nil)
	copy(arr[index+1:], arr[index:])
	arr[index] = ptr
	return arr
}

func (n *Node) insertRec(key int, value int) (splitted bool, splitKey int, leftNode *Node) {
	insert_idx := n.locateLocally(key)

	if n.isLeaf {
		record := &Record{value: value}
		if len(n.keys) == 0 {
			n.keys = append(n.keys, key)
			n.pointers = append(n.pointers, record)
		} else {
			n.keys = insertInt(insert_idx, key, n.keys)
			n.pointers = insertPtr(insert_idx, record, n.pointers)
		}
	} else {
		splitted, splitKey, leftNode := n.pointers[insert_idx].(*Node).insertRec(key, value)
		if splitted {
			n.keys = insertInt(insert_idx, splitKey, n.keys)
			n.pointers = insertPtr(insert_idx, leftNode, n.pointers)
		}
	}

	// 分割
	if n.isLeaf && len(n.keys) >= MaxDegree { // leafはn.keyの右端にInfityを含まない
		splitted = true
		splitIndex := len(n.keys) / 2
		splitKey = n.keys[splitIndex]
		// nodeの分割
		leftNode = newNodeLeaf(splitIndex)
		copy(leftNode.keys, n.keys[:splitIndex])
		copy(leftNode.pointers, n.pointers[:splitIndex])
		n.keys = n.keys[splitIndex:]
		n.pointers = n.pointers[splitIndex:]
	} else if !n.isLeaf && len(n.keys) > MaxDegree { // 中間ノードはn.keyの右端にInfityを含む
		splitted = true
		splitIndex := (len(n.keys) - 1) / 2
		splitKey = n.keys[splitIndex]
		// nodeの分割
		leftNode = newNodeNonLeaf(splitIndex)
		copy(leftNode.keys, n.keys[:splitIndex])
		copy(leftNode.pointers, n.pointers[:splitIndex])
		leftNode.keys = append(leftNode.keys, Infity)
		leftNode.pointers = append(leftNode.pointers, n.pointers[splitIndex])

		n.keys = n.keys[splitIndex+1:]
		n.pointers = n.pointers[splitIndex+1:]
	} else {
		splitted = false
	}
	return
}

type BPTree struct {
	top *Node
}

func NewBPTree() *BPTree {
	t := &BPTree{}
	return t
}
func (t *BPTree) Find(key int) (find bool, value int) {
	if key >= Infity {
		panic(errors.New("key too large"))
	}
	if t.top == nil {
		find = false
		return
	}
	return t.top.findRec(key)
}

func (t *BPTree) Insert(key int, value int) {
	if key >= Infity {
		panic(errors.New("key too large"))
	}
	if t.top == nil {
		top := newNodeNonLeaf(0)
		top.keys = append(top.keys, Infity)

		child := newNodeLeaf(0)
		child.insertRec(key, value)

		top.pointers = append(top.pointers, child)
		t.top = top
		return
	}

	splitted, splitKey, leftNode := t.top.insertRec(key, value)
	if splitted {
		top := newNodeNonLeaf(0)
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
