package query

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	bvwPool  = bsonrw.NewBSONValueWriterPool()
	buffPool = sync.Pool{New: func() interface{} {
		return &bytes.Buffer{}
	}}
)

var (
	tD = reflect.TypeOf(primitive.D{})
	tA = reflect.TypeOf(primitive.A{})
)

func Prepare(query string) (*BSONEncoder, error) {
	s := NewScanner(strings.NewReader(query))
	p := NewParser(s)

	n, err := p.Parse()
	if err != nil {
		return nil, err
	}

	return &BSONEncoder{n}, nil
}

func CompileToBSON(query string, prmMap map[string]interface{}) (CompiledQueryQuery, error) {
	enc, err := Prepare(query)
	if err != nil {
		return CompiledQueryQuery{}, err
	}

	return enc.Encode(prmMap)
}

type BSONEncoder struct {
	node *Node
}

type CompiledQueryQuery struct {
	buff *bytes.Buffer
}

func (pq CompiledQueryQuery) MarshalBSON() ([]byte, error) {
	return pq.buff.Bytes(), nil
}

func (pq CompiledQueryQuery) Close() {
	buffPool.Put(pq.buff)
}

func (enc BSONEncoder) Encode(prmMap map[string]interface{}) (CompiledQueryQuery, error) {
	buff := buffPool.Get().(*bytes.Buffer)
	buff.Reset()

	vw := bvwPool.Get(buff)
	defer bvwPool.Put(vw)

	wc := writeContext{
		vw: vw,
		ec: bsoncodec.EncodeContext{Registry: bson.DefaultRegistry},
	}

	err := encodeQuery(wc, enc.node, prmMap)
	if err != nil {
		return CompiledQueryQuery{}, err
	}

	return CompiledQueryQuery{buff}, nil
}

func encodeQuery(wc writeContext, node *Node, prmMap map[string]interface{}) error {
	dw, err := wc.vw.WriteDocument()
	if err != nil {
		return err
	}

	wc.dw = dw

	err = writeNodeDocument(wc, node, prmMap)
	if err != nil {
		return err
	}

	return dw.WriteDocumentEnd()
}

type writeContext struct {
	ec bsoncodec.EncodeContext
	dw bsonrw.DocumentWriter
	aw bsonrw.ArrayWriter
	vw bsonrw.ValueWriter
}

type docFunc func(wc writeContext) (writeContext, error)

func doc(node, parent *Node, wc writeContext) (docFunc, docFunc, docFunc) {
	pop := "and"
	if parent != nil {
		pop = parent.Op
	}

	if node.Op == "or" {
		if node.Op == pop {
			// }, {
			return emptyDoc, elemSep, emptyDoc
		}

		// $or: [ {
		// }, {
		// } ]
		return clauseStart("$or"), elemSep, clauseEnd
	}

	return emptyDoc, emptyDoc, emptyDoc
}

func emptyDoc(wc writeContext) (writeContext, error) {
	return wc, nil
}

func elemSep(wc writeContext) (writeContext, error) {
	// }, {
	err := wc.dw.WriteDocumentEnd()
	if err != nil {
		return wc, err
	}

	wc.vw, err = wc.aw.WriteArrayElement()
	if err != nil {
		return wc, err
	}

	wc.dw, err = wc.vw.WriteDocument()
	if err != nil {
		return wc, err
	}

	return wc, nil
}

func clauseEnd(wc writeContext) (writeContext, error) {
	err := wc.dw.WriteDocumentEnd()
	if err != nil {
		return wc, err
	}

	err = wc.aw.WriteArrayEnd()
	if err != nil {
		return wc, err
	}

	// wc.aw = nil
	return wc, nil
}

func clauseStart(clause string) docFunc {
	// $or: [ {
	return func(wc writeContext) (writeContext, error) {
		vw, err := wc.dw.WriteDocumentElement(clause)
		if err != nil {
			return wc, err
		}

		wc.aw, err = vw.WriteArray()
		if err != nil {
			return wc, err
		}

		wc.vw, err = wc.aw.WriteArrayElement()
		if err != nil {
			return wc, err
		}

		wc.dw, err = vw.WriteDocument()
		if err != nil {
			return wc, err
		}

		return wc, nil
	}
}

func writeNodeDocument(
	wc writeContext,
	node *Node,
	prmMap map[string]interface{},
) error {
	openDoc, sep, closeDoc := doc(node, node.Parent, wc)

	wc, err := openDoc(wc)
	if err != nil {
		return err
	}

	if node.LN != nil {
		err := writeNodeDocument(wc, node.LN, prmMap)
		if err != nil {
			return err
		}
	}

	if node.L != nil {
		err := encodeExpression(wc, node.L, prmMap)
		if err != nil {
			return err
		}
	}

	if node.RN != nil || node.R != nil {
		wc, err = sep(wc)
		if err != nil {
			return err
		}
	}

	if node.RN != nil {
		err = writeNodeDocument(wc, node.RN, prmMap)
		if err != nil {
			return err
		}
	}

	if node.R != nil {
		err := encodeExpression(wc, node.R, prmMap)
		if err != nil {
			return err
		}
	}

	_, err = closeDoc(wc)
	return err
}

func encodeExpression(
	wc writeContext,
	e *Expression,
	prmMap map[string]interface{},
) error {
	var k, v []byte

	if e.LT == TKey {
		k = e.L
	}

	if e.RT == TKey {
		k = e.R
	}

	if len(k) == 0 {
		return fmt.Errorf("no key for expression")
	}

	var vt Token

	if e.LT != TKey {
		v = e.L
		vt = e.LT
	}

	if e.RT != TKey {
		v = e.R
		vt = e.RT
	}

	return encodeElement(wc, k, v, vt, e.Op, prmMap)
}

func encodeElement(
	wc writeContext,
	k, v []byte,
	vt Token,
	op string,
	prmMap map[string]interface{},
) error {
	vw, err := wc.dw.WriteDocumentElement(string(k))
	if err != nil {
		return err
	}

	if op == "=" {
		return encodeValue(wc, v, vt, prmMap)
	} else {
		dw, err := vw.WriteDocument()
		if err != nil {
			return err
		}

		k = opKey(op)
		wc.dw = dw

		err = encodeElement(wc, k, v, vt, "=", prmMap)
		if err != nil {
			return err
		}

		return dw.WriteDocumentEnd()
	}
}

func opKey(op string) []byte {
	switch op {
	case ">":
		return []byte("$gt")
	case "<":
		return []byte("$lt")
	case ">=":
		return []byte("$gte")
	case "<=":
		return []byte("$lte")
	case "=":
		return []byte("$eq")
	}

	return []byte(op)
}

func encodeValue(
	wc writeContext,
	v []byte,
	vt Token,
	// vw bsonrw.ValueWriter,
	prmMap map[string]interface{},
) error {
	if string(v) == "null" {
		return wc.vw.WriteNull()
	}

	rv := restoreValue(v, vt)
	lv := lookupValue(rv, prmMap)

	encoder, err := lookupEncoder(wc.ec, reflect.TypeOf(lv))
	if err != nil {
		return err
	}

	err = encoder.EncodeValue(wc.ec, wc.vw, reflect.ValueOf(lv))
	if err != nil {
		return err
	}
	return nil
}

func lookupValue(v interface{}, prmMap map[string]interface{}) interface{} {
	s, ok := v.(string)
	if ok && strings.HasPrefix(s, "$") {
		pv, ok := prmMap[s]
		if ok {
			return pv
		}
	}

	return v
}

func restoreValue(v []byte, t Token) interface{} {
	switch t {
	case TString:
		return string(v[1 : len(v)-1])
	case TNumber:
		ui := binary.BigEndian.Uint64(v)
		return int64(ui)
	case TKey:
		return string(v)
	}

	return v
}

func lookupEncoder(ec bsoncodec.EncodeContext, typ reflect.Type) (bsoncodec.ValueEncoder, error) {
	if typ.ConvertibleTo(tD) {
		return nil, nil
	} else if typ.ConvertibleTo(tA) {
		return nil, nil
	}

	return ec.LookupEncoder(typ)
}
