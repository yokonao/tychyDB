package parser_test

import (
	"github.com/tychyDB/parser"
	"testing"
)

func TestParser(t *testing.T) {
	query := parser.ParseQuery([]string{"select", "id"})
	if query != "SELECT id FROM projects"{
		t.Error("expected SELECT id FROM projects")
	}
	if parser.ParseQuery([]string{"select", "id", "name"}) != "SELECT id, name FROM projects"{
		t.Error("expected SELECT id, name FROM projects")
	}
}
