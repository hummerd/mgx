package query

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

var (
	keyOr           = []byte("or")
	keyAnd          = []byte("and")
	keyFuncDate     = []byte("ISODate")
	keyFuncObjectID = []byte("ObjectId")
)

var ErrParsed = errors.New("text parsed")

func NewParser(s *Scanner) *Parser {
	return &Parser{
		s: s,
	}
}

type Parser struct {
	s *Scanner
}

func (p *Parser) Parse() (*Node, error) {
	root := &Node{LRoot: true}
	n := &Node{Op: "and", Parent: root}
	root.LN = n

	for {
		var err error

		n, err = p.parse(n)
		if err != nil {
			if errors.Is(err, ErrParsed) {
				r := root.LN
				r.Parent = nil

				Link(r)
				r = Reduce(r)

				return r, nil
			}

			return nil, err
		}
	}
}

func (p *Parser) parse(n *Node) (*Node, error) {
	t, l, err := p.readToken(true, "")
	if err != nil {
		return nil, err
	}

	switch t {
	case TKey, TNumber, TString:
		switch {
		case bytes.EqualFold(l, keyAnd):
			if n.L == nil && n.LN == nil {
				line, column := p.s.Position()
				return nil, fmt.Errorf("unexpected 'and' at: line:%d; column: %d", line, column)
			}

			newN := &Node{Op: "and", Parent: n}
			n.SetNextNode(newN)
			return newN, nil
		case bytes.EqualFold(l, keyOr):
			if n.L == nil && n.LN == nil {
				line, column := p.s.Position()
				return nil, fmt.Errorf("unexpected 'or' at: line:%d; column: %d", line, column)
			}

			r := n.LocalRoot()

			newN := &Node{Op: "or", Parent: r}
			newN.LN = r.LN
			newN.LN.Parent = newN
			r.LN = newN

			return newN, nil
		}

		e, err := p.parseExpression(t, l)
		if err != nil {
			return nil, err
		}

		n.SetNextExpression(&e)
		return n, nil

	case TParentheses:
		if l[0] == ')' {
			if n.Parent.Parent == nil {
				line, column := p.s.Position()
				return nil, fmt.Errorf("unexpected ')' at: line:%d; column: %d", line, column)
			}

			n = n.Parent
			return n, nil
		}

		newN := &Node{Op: "and", Parent: n}
		n.SetNextNode(newN)
		n.LRoot = true
		return newN, nil
	}

	line, column := p.s.Position()
	return nil, fmt.Errorf("unexpected token at: line:%d; column: %d", line, column)
}

func token(t Token, in ...Token) bool {
	for _, tin := range in {
		if tin == t {
			return true
		}
	}

	return false
}

func (p *Parser) readToken(canBeEnd bool, unexpected string) (Token, []byte, error) {
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
	return t, l, nil
}

func (p *Parser) readAndCheckToken(canBeEnd bool, unexpected string, tokens ...Token) (Token, []byte, error) {
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

func (p *Parser) parseExpression(startT Token, startL []byte) (Expression, error) {
	var e Expression

	e.L, e.LT = p.tokenValue(startT, startL)
	// e.S = p.s.Position()

	_, l, err := p.readAndCheckToken(false, "unexpected end of expression", TOp, TKey)
	if err != nil {
		return e, err
	}

	e.Op = string(l)

	t, l, err := p.readAndCheckToken(false, "unexpected end of expression", TKey, TNumber, TString, TRegex)
	if err != nil {
		return e, err
	}

	e.R, e.RT = p.tokenValue(t, l)

	return e, nil
}

func (p *Parser) tokenValue(t Token, l []byte) ([]byte, ValueType) {
	switch t {
	case TString:
		return append([]byte(nil), l...), VTString
	case TRegex:
		return append([]byte(nil), l...), VTRegex
	case TNumber:
		n, _ := strconv.Atoi(string(l))
		return binary.BigEndian.AppendUint64(nil, uint64(n)), VTNumber
	case TKey:
		switch {
		case bytes.Equal(l, keyFuncObjectID):
			return p.parseFuncObjectID()
		case bytes.Equal(l, keyFuncDate):
			return p.parseFuncDate()
		default:
			return append([]byte(nil), l...), VTKey
		}
	}

	return nil, 0
}

func (p *Parser) parseFuncObjectID() ([]byte, ValueType) {
	_, op, err := p.readAndCheckToken(false, "unexpected end of script", TParentheses)
	if err != nil {
		return nil, 0
	}

	if len(op) != 1 || op[0] != '(' {
		return nil, 0
	}

	_, v, err := p.readAndCheckToken(false, "unexpected end of script", TString)
	if err != nil {
		return nil, 0
	}

	v = v[1 : len(v)-1]
	h := make([]byte, hex.DecodedLen(len(v)))
	_, _ = hex.Decode(h, v)

	_, cp, err := p.readAndCheckToken(false, "unexpected end of script", TParentheses)
	if err != nil {
		return nil, 0
	}

	if len(cp) != 1 || cp[0] != ')' {
		return nil, 0
	}

	return h, VTObjectID
}

func (p *Parser) parseFuncDate() ([]byte, ValueType) {
	_, op, err := p.readAndCheckToken(false, "unexpected end of script", TParentheses)
	if err != nil {
		return nil, 0
	}

	if len(op) != 1 || op[0] != '(' {
		return nil, 0
	}

	_, v, err := p.readAndCheckToken(false, "unexpected end of script", TString)
	if err != nil {
		return nil, 0
	}

	dts := string(v[1 : len(v)-1])

	_, cp, err := p.readAndCheckToken(false, "unexpected end of script", TParentheses)
	if err != nil {
		return nil, 0
	}

	if len(cp) != 1 || cp[0] != ')' {
		return nil, 0
	}

	dt, _ := time.Parse(time.RFC3339Nano, dts)
	return binary.BigEndian.AppendUint64(nil, uint64(dt.UnixMilli())), VTDate
}
