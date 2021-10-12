package btree_test

import (
	"testing"

	"github.com/tychyDB/btree"
)

func TestBTree(t *testing.T) {
	tree := btree.NewTree(3)
	tree.Insert(10)
	tree.Insert(20)
	tree.Insert(30)
	tree.Insert(32)
	tree.Insert(11)
	tree.Insert(9)

	//tree.Print()
	tree.Insert(100)

	//tree.Print()
	tree.Insert(103)

	//tree.Print()
	tree.Insert(108)

	//tree.Print()
	tree.Insert(111)

	//tree.Print()
	tree.Insert(150)

	//tree.Print()
	tree.Insert(80)
	//tree.Print()
}
