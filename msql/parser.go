package msql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
)

var (
	keyOr  = []byte("or")
	keyAnd = []byte("and")
)

var ErrParsed = errors.New("text parsed")

type NodeContext struct {
	Field []byte
}

type Node struct {
	Parent *Node
	In     *NodeContext
	Op     string
	L, R   *Expression
	LN, RN *Node
	LRoot  bool
}

func (n *Node) String() string {
	if n == nil {
		return "<nil>"
	}

	l := ""
	if n.L != nil {
		l = n.L.String()
	}
	if n.LN != nil {
		l = n.LN.String()
	}

	r := ""
	if n.R != nil {
		r = n.R.String()
	}
	if n.RN != nil {
		r = n.RN.String()
	}

	return fmt.Sprintf("(%s) %s (%s)", l, n.Op, r)
}

func (n *Node) FixParent() {
	if n.LN != nil {
		n.LN.FixParent()
		n.LN.Parent = n
	}

	if n.RN != nil {
		n.RN.FixParent()
		n.RN.Parent = n
	}
}

func (n *Node) Root() *Node {
	for {
		if n.Parent == nil {
			return n
		}

		n = n.Parent
	}
}

func (n *Node) LocalRoot() *Node {
	for {
		if n.LRoot {
			return n
		}

		n = n.Parent
	}
}

func (n *Node) SetNextExpression(ne *Expression) {
	if n.L == nil && n.LN == nil {
		n.L = ne
	} else {
		n.R = ne
	}
}

func (n *Node) SetNextNode(nn *Node) {
	if n.L == nil && n.LN == nil {
		n.LN = nn
	} else {
		n.RN = nn
	}
}

func (n *Node) Replace(on, nn *Node) {
	if n.LN == on {
		n.LN = nn
	} else if n.RN == on {
		n.RN = nn
	}
}

func (n *Node) Reduce() (*Node, *Expression) {
	if n.LN != nil {
		n.LN, n.L = n.LN.Reduce()
	}

	if n.RN != nil {
		n.RN, n.R = n.RN.Reduce()
	}

	le := n.LN == nil && n.L == nil
	re := n.RN == nil && n.R == nil

	if le && re {
		return nil, nil
	}

	if !le && !re {
		return n, nil
	}

	if !le {
		if n.LN != nil {
			return n.LN, nil
		}

		return nil, n.L
	}

	if !re {
		if n.RN != nil {
			return n.RN, nil
		}

		return nil, n.R
	}

	return n, nil
}

type Expression struct {
	Op   string
	L    []byte
	LT   Token
	R    []byte
	RT   Token
	S, T pos
}

func (e *Expression) String() string {
	return fmt.Sprintf("%s %s %s", e.L, e.Op, e.R)
}

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

				r.Reduce()
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

	// t, l, err := p.readAndCheckToken(true, "", TKey, TNumber, TString)
	// if err != nil {
	// 	return e, err
	// }

	e.L = tokenValue(startT, startL)
	e.LT = startT
	// e.S = p.s.Position()

	_, l, err := p.readAndCheckToken(false, "unexpected end of expression", TOp, TKey)
	if err != nil {
		return e, err
	}

	e.Op = string(l)

	t, l, err := p.readAndCheckToken(false, "unexpected end of expression", TKey, TNumber, TString)
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