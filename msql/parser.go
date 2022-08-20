package msql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var ErrParsed = errors.New("text parsed")

type NodeContext struct {
	Field []byte
}

type Node struct {
	In     *NodeContext
	Op     string
	L, R   *Expression
	LN, RN *Node
}

type Expression struct {
	Op   string
	L    []byte
	LT   Token
	R    []byte
	RT   Token
	S, T pos
}

// type Query struct {
// 	qc    *QueryContext
// 	Conds []Expression
// }

func NewParser(s *Scanner) *Parser {
	return &Parser{
		s: s,
	}
}

type Parser struct {
	s *Scanner
}

// a & b & c
// a & b | c

// a and b or c and d

//                and
//          or          d
//   and        c
// a     b

//          or
//     and       and
// a       b   c      d

// a and b or c o
//                or
//          or         d
//     and       and
// a       b   c      d

//                      or
//    and
//  a      and
//        b     and
//                  c

func (p *Parser) Parse() (*Node, error) {
	n := &Node{Op: "and"}
	root := n

	var pn *Node

	for {
		e, err := p.parseExpression()
		if err != nil {
			if errors.Is(err, ErrParsed) {
				if n.L == nil {
					line, column := p.s.Position()
					return nil, fmt.Errorf("unexpected EOF at: line:%d; column: %d", line, column)
				}
				return root, nil
			}
		}

		if n.L == nil && n.LN == nil {
			n.L = &e
		} else {
			n.R = &e
		}

		_, l, err := p.readToken(true, "", TKey)
		if err != nil {
			if errors.Is(err, ErrParsed) {
				if pn != nil {
					pn.R = n.L
					pn.RN = nil
				}
				return root, nil
			}
		}

		if strings.EqualFold("and", string(l)) {
			newN := &Node{Op: "and"}
			n.RN = newN
			pn = n
			n = newN
		} else if strings.EqualFold("or", string(l)) {
			if pn != nil {
				pn.R = n.L
				pn.RN = nil
			}

			newN := &Node{Op: "or"}
			newN.LN = root
			pn = nil
			n = newN
			root = newN
		} else {
			line, column := p.s.Position()
			return nil, fmt.Errorf("unexpected token at: line:%d; column: %d", line, column)
		}
	}
}

func token(t Token, in ...Token) bool {
	for _, tin := range in {
		if tin == t {
			return true
		}
	}

	return false
}

func (p *Parser) readToken(canBeEnd bool, unexpected string, tokens ...Token) (Token, []byte, error) {
	err := p.s.Next()
	if err != nil {
		if canBeEnd && errors.Is(err, io.EOF) {
			if canBeEnd {
				return 0, nil, ErrParsed
			}

			line, column := p.s.Position()
			return 0, nil, fmt.Errorf("%s: line:%d; column: %d", unexpected, line, column)
		}

		return 0, nil, err
	}

	t, l := p.s.Token()
	if !token(t, tokens...) {
		line, column := p.s.Position()
		return 0, nil, fmt.Errorf("unexpected symbol %s at position: line:%d; column: %d", l, line, column)
	}

	return t, l, nil
}

func (p *Parser) parseExpression() (Expression, error) {
	var e Expression

	t, l, err := p.readToken(true, "", TKey, TNumber, TString)
	if err != nil {
		return e, err
	}

	e.L = tokenValue(t, l)
	e.LT = t
	// e.S = p.s.Position()

	_, l, err = p.readToken(false, "unexpected end of expression", TOp, TKey)
	if err != nil {
		return e, err
	}

	e.Op = string(l)

	t, l, err = p.readToken(false, "unexpected end of expression", TKey, TNumber, TString)
	if err != nil {
		return e, err
	}

	e.R = tokenValue(t, l)
	e.RT = t

	return e, nil
}

func tokenValue(t Token, l []byte) []byte {
	switch t {
	case TString:
		return append([]byte(nil), l...)
	case TNumber:
		n, _ := strconv.Atoi(string(l))
		return binary.BigEndian.AppendUint64(nil, uint64(n))
	case TKey:
		return append([]byte(nil), l...)
	}

	return nil
}
