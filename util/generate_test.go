package util_test

import (
	"testing"

	"github.com/tychyDB/util"
)

func TestGenUInt32(t *testing.T) {
	gs := util.NewGenStruct(0, 100)
	gs.PutUInt32(10)
	gs.PutUInt32(11)
	gs.PutUInt32(111)

	// should avoid IterStrcut?
	iter := util.NewIterStruct(0, gs.DumpBytes())

	if i := iter.NextUInt32(); i != 10 {
		t.Errorf("expected %d, but got %d", 10, i)
	}

	if i := iter.NextUInt32(); i != 11 {
		t.Errorf("expected %d, but got %d", 11, i)
	}

	if i := iter.NextUInt32(); i != 111 {
		t.Errorf("expected %d, but got %d", 111, i)
	}
}

func TestGenBool(t *testing.T) {
	gs := util.NewGenStruct(0, 100)
	gs.PutUInt32(10)
	gs.PutBool(false)
	gs.PutUInt32(11)
	gs.PutBool(true)
	gs.PutUInt32(111)
	gs.PutBool(true)

	// should avoid IterStrcut?
	iter := util.NewIterStruct(0, gs.DumpBytes())

	if i := iter.NextUInt32(); i != 10 {
		t.Errorf("expected %d, but got %d", 10, i)
	}

	if i := iter.NextBool(); i {
		t.Errorf("expected %v, but got %v", false, i)
	}

	if i := iter.NextUInt32(); i != 11 {
		t.Errorf("expected %d, but got %d", 11, i)
	}

	if i := iter.NextBool(); !i {
		t.Errorf("expected %v, but got %v", true, i)
	}

	if i := iter.NextUInt32(); i != 111 {
		t.Errorf("expected %d, but got %d", 111, i)
	}

	if i := iter.NextBool(); !i {
		t.Errorf("expected %v, but got %v", true, i)
	}

}

func TestGenBytes(t *testing.T) {
	t_gs := util.NewGenStruct(0, 12)
	t_gs.PutUInt32(10)
	t_gs.PutUInt32(11)
	t_gs.PutUInt32(111)

	gs := util.NewGenStruct(0, 3232)
	gs.PutUInt32(11)
	gs.PutBytes(12, t_gs.DumpBytes())
	gs.PutUInt32(22)
	gs.PutBytes(12, t_gs.DumpBytes())

	// should avoid IterStrcut?
	iter := util.NewIterStruct(0, gs.DumpBytes())

	if i := iter.NextUInt32(); i != 11 {
		t.Errorf("expected %d, but got %d", 11, i)
	}

	if i := iter.NextUInt32(); i != 10 {
		t.Errorf("expected %d, but got %d", 10, i)
	}

	if i := iter.NextUInt32(); i != 11 {
		t.Errorf("expected %d, but got %d", 11, i)
	}

	if i := iter.NextUInt32(); i != 111 {
		t.Errorf("expected %d, but got %d", 111, i)
	}

	if i := iter.NextUInt32(); i != 22 {
		t.Errorf("expected %d, but got %d", 22, i)
	}

	if i := iter.NextUInt32(); i != 10 {
		t.Errorf("expected %d, but got %d", 10, i)
	}

	if i := iter.NextUInt32(); i != 11 {
		t.Errorf("expected %d, but got %d", 11, i)
	}

	if i := iter.NextUInt32(); i != 111 {
		t.Errorf("expected %d, but got %d", 111, i)
	}
}
