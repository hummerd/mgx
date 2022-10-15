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

func newLinkMap() linkMap {
	return make(map[string]*Expression)
}

type linkMap map[string]*Expression

// linkNode finds expressions that refers same key.
// All linked expressions gathered to firs expression, and removed from
// original nodes.
func (lm linkMap) linkNode(n *Node) {
	if n.L != nil {
		linked := lm.linkExpression(n.L)
		if linked {
			n.L = nil
		}
	}

	if n.R != nil {
		linked := lm.linkExpression(n.R)
		if linked {
			n.R = nil
		}
	}

	if n.LN != nil {
		llm := lm
		if n.Op == "or" {
			llm = newLinkMap()
		}

		llm.linkNode(n.LN)
	}

	if n.RN != nil {
		llm := lm

		if n.Op == "or" {
			llm = newLinkMap()
		}

		llm.linkNode(n.RN)
	}
}

func (lm linkMap) linkExpression(e *Expression) bool {
	k := e.FindKey()

	if k == nil {
		return false
	}

	ee, ok := lm[string(k)]
	if ok {
		if ee.Links == nil {
			ee.Links = &[]*Expression{}
		}
		*ee.Links = append(*ee.Links, e)
		return true
	}

	lm[string(k)] = e
	return false
}

func (n *Node) Link() {
	lm := newLinkMap()
	lm.linkNode(n)
}

func (n *Node) Reduce() (*Node, *Expression) {
	if n.LN != nil {
		n.LN, n.L = n.LN.Reduce()
	}

	if n.RN != nil {
		n.RN, n.R = n.RN.Reduce()
	}

	lempty := n.LN == nil && n.L == nil
	rempty := n.RN == nil && n.R == nil

	if lempty && rempty {
		return nil, nil
	}

	if !lempty && !rempty {
		return n, nil
	}

	if !lempty {
		if n.LN != nil {
			return n.LN, nil
		}

		return nil, n.L
	}

	if !rempty {
		if n.RN != nil {
			return n.RN, nil
		}

		return nil, n.R
	}

	return n, nil
}

type ValueType uint

const (
	VTKey ValueType = iota + 1
	VTNumber
	VTString
	VTRegex
	VTDate
	VTObjectID
)

type Expression struct {
	Op    string
	L     []byte
	LT    ValueType
	R     []byte
	RT    ValueType
	S, T  pos
	Links *[]*Expression
}

func (e *Expression) FindKey() []byte {
	if e.LT == VTKey {
		return e.L
	}

	if e.RT == VTKey {
		return e.R
	}

	return nil
}

func (e *Expression) String() string {
	return fmt.Sprintf("%X %s %X links: %v",
		e.L, e.Op, e.R, e.Links)
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

				r.Link()

				rn, _ := r.Reduce()
				if rn != nil {
					r = rn
				}
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
