package storage

type Type struct {
	name string
	size uint
}

func (t Type) String() string {
	return t.name
}
