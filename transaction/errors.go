package transaction

import "errors"

var (
	ErrExist       = errors.New("already exists")
	ErrNotExist    = errors.New("not exists")
	ErrOutOfBounds = errors.New("out of bounds")
)
