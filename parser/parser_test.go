package parser_test

import (
	"github.com/tychyDB/parser"
	"testing"
)


func TestTokenizerSingle(t *testing.T) {
	tok := parser.Tokenize("'hoge'")[0]
	if tok.Kind != parser.VALUE {
		t.Error("expected TokenKind to be VALUE")
	}
	if tok.Str != "hoge" {
		t.Errorf("expected: hoge\nactual: %v\n", tok.Str)
	}
	tokB := parser.Tokenize("=")[0]
	if tokB.Kind != parser.OPERATOR {
		t.Error(("\nexpected TokenKind to be OPERATOR"))
	}
	if tokB.Str != "=" {
		t.Errorf("\nexpected: =\nactual: %v\n", tokB.Str)
	}
	tokC := parser.Tokenize("hoge")[0]
	if tokC.Kind != parser.IDENT {
		t.Error("expected TokenKind to be IDENT")
	}
	if tokC.Str != "hoge" {
		t.Errorf("\nexpected: hoge\nactual: %v\n", tokC.Str)
	}
}

func TestTokenizerSimpleSQL(t *testing.T) {
	tokens := parser.Tokenize("SELECT capital FROM world WHERE name = 'France'")
	if len(tokens) != 8 {
		t.Errorf("\nexpected: 8\nactual: %d\n", len(tokens))
	}
	if tokens[0].Kind != parser.IDENT{
		t.Errorf("expected: IDENT, actual: %s", tokens[0].Kind.String())
	}
	if tokens[0].Str != "select"{
		t.Errorf("expected: select, actual: %s", tokens[0].Str)
	}
	if tokens[7].Kind != parser.VALUE{
		t.Errorf("expected: VALUE, actual: %s", tokens[7].Kind.String())
	}
	if tokens[7].Str != "france"{
		t.Errorf("expected: france, actual: %s", tokens[7].Str)
	}
}
