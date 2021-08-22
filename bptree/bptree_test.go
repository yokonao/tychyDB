package bptree_test

import (
	"fmt"
	"testing"

	"github.com/tychyDB/bptree"
)

func TestBPTree(t *testing.T) {
	tree := bptree.NewBPTree()

	tree.Insert(10, 1)
	tree.Print()

	tree.Insert(11, 188)
	tree.Print()

	tree.Insert(12, 1)
	tree.Print()

	tree.Insert(100, 1)
	tree.Print()

	tree.Insert(101, 1)
	tree.Print()

	tree.Insert(1000, 1)
	tree.Print()

	tree.Insert(1, 11)
	tree.Print()

	tree.Insert(2, 1)
	tree.Print()

	tree.Insert(3, 11)
	tree.Print()

	tree.Insert(998, 100)
	tree.Print()

	find, value := tree.Find(3)
	fmt.Printf("key %d find %v value %d\n", 3, find, value)

	find, value = tree.Find(998)
	fmt.Printf("key %d find %v value %d\n", 998, find, value)

	find, value = tree.Find(1)
	fmt.Printf("key %d find %v value %d\n", 1, find, value)

	find, value = tree.Find(11)
	fmt.Printf("key %d find %v value %d\n", 11, find, value)

	find, value = tree.Find(17)
	fmt.Printf("key %d find %v value %d\n", 17, find, value)

	find, value = tree.Find(7)
	fmt.Printf("key %d find %v value %d\n", 7, find, value)
}
