package query

import (
	"fmt"
)

type ValueType uint

const (
	VTKey ValueType = iota + 1
	VTInteger
	VTFloat
	VTString
	VTRegex
	VTDate
	VTObjectID
	VTBool
	VTArray
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

func Reduce(n *Node) *Node {
	rn, _ := reduce(n)
	if rn != nil {
		return rn
	}

	return n
}

func reduce(n *Node) (*Node, *Expression) {
	if n.LN != nil {
		n.LN, n.L = reduce(n.LN)
	}

	if n.RN != nil {
		n.RN, n.R = reduce(n.RN)
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

func Link(n *Node) {
	lm := newLinkMap()
	lm.linkNode(n)
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
