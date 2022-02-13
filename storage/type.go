package storage

type TypeId uint32

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

var IntergerType Type = Type{id: integerId, size: 4}

const maxCharLen = 255

func CharType(cap uint32) Type {
	if cap > maxCharLen {
		panic("maximum char size is 255. specify less than that.")
	}
	return Type{id: charId, size: cap}
}
