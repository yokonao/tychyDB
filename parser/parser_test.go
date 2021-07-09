package parser

import (
	"testing"
)

func TestParser(t *testing.T) {
	query := parseQuery([]string{"select", "id"})
	if query != "SELECT id FROM projects"{
		t.Error("expected SELECT id FROM projects")
	}
	if parseQuery([]string{"select", "id", "name"}) != "SELECT id, name FROM projects"{
		t.Error("expected SELECT id, name FROM projects")
	}
}
