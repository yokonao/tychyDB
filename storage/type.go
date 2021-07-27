package storage

type TypeId int

const (
	integerId = iota
	charId
)

func (id TypeId) String() string {
	switch id {
	case integerId:
		return "INTEGER"
	case charId:
		return "CHAR"
	default:
		return "Unknown"
	}
}

type Type struct {
	id   TypeId
	size uint
}

func (t Type) String() string {
	return t.id.String()
}

var intergerType Type = Type{id: integerId, size: 4}
