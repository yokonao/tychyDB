package bptree_test

import (
	"testing"

	"github.com/tychyDB/bptree"
)

func TestBPTree(t *testing.T) {
	tree := bptree.NewBPTree()

	tree.Insert(10, 1)
	tree.Print()

	tree.Insert(11, 1)
	tree.Print()

	tree.Insert(12, 1)
	tree.Print()

	tree.Insert(100, 1)
	tree.Print()

	tree.Insert(101, 1)
	tree.Print()

	tree.Insert(1000, 1)
	tree.Print()

	tree.Insert(1, 1)
	tree.Print()

	tree.Insert(2, 1)
	tree.Print()

	tree.Insert(3, 1)
	tree.Print()

	tree.Insert(998, 1)
	tree.Print()
}
