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
	size uint32
}

func (t Type) String() string {
	return t.id.String()
}

var intergerType Type = Type{id: integerId, size: 4}

const maxCharLen = 255

func charType(len uint32) Type {
	if len > maxCharLen {
		panic("maximum char size is 255. specify less than that.")
	}
	return Type{id: charId, size: 4 * len}
}
