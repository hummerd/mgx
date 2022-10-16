package query

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
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

	switch {
	case IsPrimitiveOrKey(t):
		switch {
		case bytes.EqualFold(l, keyAnd):
			if n.L == nil && n.LN == nil {
				return nil, p.unexpectedSymbolError(l)
			}

			newN := &Node{Op: "and", Parent: n}
			n.SetNextNode(newN)
			return newN, nil
		case bytes.EqualFold(l, keyOr):
			if n.L == nil && n.LN == nil {
				return nil, p.unexpectedSymbolError(l)
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

	case t == TParentheses:
		if l[0] == ')' {
			if n.Parent.Parent == nil {
				return nil, p.unexpectedSymbolError(l)
			}

			n = n.Parent
			return n, nil
		}

		newN := &Node{Op: "and", Parent: n}
		n.SetNextNode(newN)
		n.LRoot = true
		return newN, nil
	}

	return nil, p.unexpectedSymbolError(l)
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

			return 0, nil, p.positionError(unexpected)
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

			return 0, nil, p.positionError(unexpected)
		}

		return 0, nil, err
	}

	t, l := p.s.Token()
	if !token(t, tokens...) {
		return 0, nil, p.unexpectedSymbolError(l)
	}

	return t, l, nil
}

func (p *Parser) parseExpression(startT Token, startL []byte) (Expression, error) {
	var e Expression
	var err error

	e.L, e.LT, err = p.tokenValue(startT, startL)
	if err != nil {
		return e, err
	}

	// e.S = p.s.Position()

	_, l, err := p.readAndCheckToken(false, "unexpected end of expression", TOp, TKey)
	if err != nil {
		return e, err
	}

	e.Op = string(l)

	if e.Op == "$in" {
		v, vt, err := p.readArray()
		if err != nil {
			return e, err
		}

		e.R, e.RT = v, vt
		return e, nil
	}

	t, l, err := p.readAndCheckToken(false, "unexpected end of expression", PrimitiveTypesAndKey...)
	if err != nil {
		return e, err
	}

	e.R, e.RT, err = p.tokenValue(t, l)
	if err != nil {
		return e, err
	}

	return e, nil
}

func (p *Parser) readArray() ([]byte, ValueType, error) {
	var buff []byte

	_, l, err := p.readAndCheckToken(false, "expected '['", TParentheses)
	if err != nil {
		return nil, 0, err
	}

	if l[0] != '[' {
		return nil, 0, p.positionError("expected '['")
	}

	c := uint32(0)
	buff = binary.BigEndian.AppendUint32(buff, c)

	for {
		// read value
		t, l, err := p.readAndCheckToken(false, "unexpected symbol (expected value for array)", PrimitiveTypesAndKey...)
		if err != nil {
			return nil, 0, err
		}

		buff, err = p.encodeBinaryToken(buff, t, l)
		if err != nil {
			return nil, 0, err
		}

		c++

		// read comma or closing bracket
		t, l, err = p.readAndCheckToken(false, "unexpected symbol (expected ',' or ']')", TComma, TParentheses)
		if err != nil {
			return nil, 0, err
		}

		if t == TComma {
			continue
		}

		if t == TParentheses && l[0] == ']' {
			break
		}

		return nil, 0, p.unexpectedSymbolError(l)
	}

	binary.BigEndian.PutUint32(buff, c)
	return buff, VTArray, nil
}

func (p *Parser) encodeBinaryToken(buff []byte, t Token, l []byte) ([]byte, error) {
	tv, vt, err := p.tokenValue(t, l)
	if err != nil {
		return nil, err
	}

	buff = append(buff, byte(vt))

	tl := p.tokenLength(vt, tv)
	if tl != nil {
		buff = binary.BigEndian.AppendUint32(buff, uint32(*tl))
	}

	return append(buff, tv...), nil
}

func (p *Parser) tokenValue(t Token, l []byte) ([]byte, ValueType, error) {
	var v []byte
	var vt ValueType
	var err error

	switch t {
	case TString:
		v, vt = append([]byte(nil), l...), VTString
	case TRegex:
		v, vt = append([]byte(nil), l...), VTRegex
	case TNumber:
		v, vt, err = parseNumber(l)
	case TBool:
		v, vt = parseBool(l)
	case TKey:
		switch {
		case bytes.Equal(l, keyFuncObjectID):
			v, vt, err = p.parseFuncObjectID()
		case bytes.Equal(l, keyFuncDate):
			v, vt, err = p.parseFuncDate()
		default:
			v, vt, err = append([]byte(nil), l...), VTKey, nil
		}
	}

	if err != nil {
		return nil, 0, p.positionError(err.Error())
	}

	return v, vt, nil
}

func (p *Parser) tokenLength(vt ValueType, l []byte) *int32 {
	switch vt {
	case VTString, VTRegex, VTKey:
		len := int32(len(l))
		return &len
	case VTObjectID, VTInteger, VTFloat, VTBool, VTDate:
		return nil
	}

	return nil
}

func (p *Parser) parseFuncObjectID() ([]byte, ValueType, error) {
	_, op, err := p.readAndCheckToken(false, "unexpected end of script", TParentheses)
	if err != nil {
		return nil, 0, err
	}

	if len(op) != 1 || op[0] != '(' {
		return nil, 0, fmt.Errorf("unexpected %s", op)
	}

	_, v, err := p.readAndCheckToken(false, "unexpected end of script", TString)
	if err != nil {
		return nil, 0, err
	}

	v = v[1 : len(v)-1]
	h := make([]byte, hex.DecodedLen(len(v)))
	_, err = hex.Decode(h, v)
	if err != nil {
		return nil, 0, err
	}

	_, cp, err := p.readAndCheckToken(false, "unexpected end of script", TParentheses)
	if err != nil {
		return nil, 0, err
	}

	if len(cp) != 1 || cp[0] != ')' {
		return nil, 0, fmt.Errorf("unexpected %s", cp)
	}

	return h, VTObjectID, nil
}

func (p *Parser) parseFuncDate() ([]byte, ValueType, error) {
	_, op, err := p.readAndCheckToken(false, "unexpected end of script", TParentheses)
	if err != nil {
		return nil, 0, err
	}

	if len(op) != 1 || op[0] != '(' {
		return nil, 0, fmt.Errorf("unexpected %s", op)
	}

	_, v, err := p.readAndCheckToken(false, "unexpected end of script", TString)
	if err != nil {
		return nil, 0, err
	}

	dts := string(v[1 : len(v)-1])

	_, cp, err := p.readAndCheckToken(false, "unexpected end of script", TParentheses)
	if err != nil {
		return nil, 0, err
	}

	if len(cp) != 1 || cp[0] != ')' {
		return nil, 0, fmt.Errorf("unexpected %s", cp)
	}

	dt, err := time.Parse(time.RFC3339Nano, dts)
	if err != nil {
		return nil, 0, err
	}

	return binary.BigEndian.AppendUint64(nil, uint64(dt.UnixMilli())), VTDate, nil
}

func parseNumber(l []byte) ([]byte, ValueType, error) {
	if bytes.Contains(l, []byte{'.'}) {
		f, err := strconv.ParseFloat(string(l), 64)
		if err != nil {
			return nil, 0, err
		}

		return binary.BigEndian.AppendUint64(nil, math.Float64bits(f)), VTFloat, nil
	}

	n, err := strconv.Atoi(string(l))
	if err != nil {
		return nil, 0, err
	}

	return binary.BigEndian.AppendUint64(nil, uint64(n)), VTInteger, nil
}

func parseBool(l []byte) ([]byte, ValueType) {
	if l[0] == 't' || l[0] == 'T' {
		return []byte{1}, VTBool
	}

	return []byte{0}, VTBool
}

func (p *Parser) positionError(msg string) error {
	line, column := p.s.Position()
	return fmt.Errorf("%s: line %d; column %d", msg, line, column)
}

func (p *Parser) unexpectedSymbolError(sym []byte) error {
	line, column := p.s.Position()
	return fmt.Errorf("unexpected symbol %s at position: line %d; column %d", sym, line, column)
}
