// SELECT capital
//   FROM world
//   WHERE name = 'France'

// 基本的に大文字小文字の区別はしない

package parser

import (
	"fmt"
	"strings"
	"unicode"
)

type TokenKind int

const (
	IDENT = iota
	OPERATOR
	VALUE
)

func (tk TokenKind) String() string {
	switch tk {
	case IDENT:
		return "IDENT"
	case OPERATOR:
		return "OPERATOR"
	case VALUE:
		return "VALUE"
	default:
		return "Not Implemented"
	}
}

func isSmallAlphabet(r rune) bool {
	return r >= 'a' && r <= 'z'
}

func isOperator(r rune) bool {
	for _, op := range "=" {
		if r == op {
			return true
		}
	}
	return false
}

type Token struct {
	Kind TokenKind
	Str  string
}

func (tok Token) String() string {
	return fmt.Sprintf("{ Kind: %s, Str: %s }", tok.Kind.String(), tok.Str)
}

func Tokenize(src string) []Token {
	var res []Token
	src = strings.ToLower(src)
	runes := []rune(src)
	runes = append(runes, ' ')
	for i := 0; i < len(runes); i++ {
		if unicode.IsSpace(runes[i]) {
			continue
		}

		if runes[i] == '\'' {
			j := 1
			for ; runes[i+j] != '\''; j++ {
			}
			res = append(res, Token{Kind: VALUE, Str: string(runes[i+1 : i+j])})
			i += j
			continue
		}

		if isOperator(runes[i]) {
			res = append(res, Token{Kind: OPERATOR, Str: string(runes[i])})
			continue
		}

		if isSmallAlphabet(runes[i]) {
			j := 1
			for ; isSmallAlphabet(runes[i+j]); j++ {
			}
			res = append(res, Token{Kind: IDENT, Str: string(runes[i : i+j])})
			i += j
			continue
		}
	}
	for _, tok := range res {
		fmt.Println(tok.String())
	}
	return res
}
