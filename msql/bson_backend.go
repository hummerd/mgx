package msql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/hummerd/mgx"
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

func CompileToBSON(query string, prmMap map[string]interface{}) (mgx.MarshalledQuery, error) {
	s := NewScanner(strings.NewReader(query))
	p := NewParser(s)

	n, err := p.Parse()
	if err != nil {
		return mgx.MarshalledQuery{}, err
	}

	buff := buffPool.New().(*bytes.Buffer)
	buff.Reset()

	vw := bvwPool.Get(buff)
	defer bvwPool.Put(vw)

	wc := writeContext{
		vw: vw,
		ec: bsoncodec.EncodeContext{Registry: bson.DefaultRegistry},
	}

	enc := NodeEncoder{prmMap}
	err = enc.encodeQuery(n, wc)
	if err != nil {
		return mgx.MarshalledQuery{}, err
	}

	return mgx.NewMarshalledQuery(buff), nil
}

type NodeEncoder struct {
	prmMap map[string]interface{}
}

// $or : [
// 	{}
// 	{}
// 	{ $and: [

// 	] }
// ]

func (enc NodeEncoder) encodeQuery(n *Node, wc writeContext) error {
	dw, err := wc.vw.WriteDocument()
	if err != nil {
		return err
	}

	wc.dw = dw

	err = enc.writeNodeDocument(wc, n, nil)
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

func (enc NodeEncoder) writeNodeDocument(wc writeContext, node, parent *Node) error {
	openDoc, sep, closeDoc := doc(node, parent, wc)

	wc, err := openDoc(wc)
	if err != nil {
		return err
	}

	if node.LN != nil {
		err := enc.writeNodeDocument(wc, node.LN, node)
		if err != nil {
			return err
		}
	}

	if node.L != nil {
		err := enc.encodeExpression(wc, node.L)
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
		err = enc.writeNodeDocument(wc, node.RN, node)
		if err != nil {
			return err
		}
	}

	if node.R != nil {
		err := enc.encodeExpression(wc, node.R)
		if err != nil {
			return err
		}
	}

	_, err = closeDoc(wc)
	return err
}

func (enc NodeEncoder) encodeExpression(wc writeContext, e *Expression) error {
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

	return enc.encodeElement(wc, k, v, vt, e.Op)
}

func (enc NodeEncoder) encodeElement(wc writeContext, k, v []byte, vt Token, op string) error {
	qk := fmt.Sprintf(`"%s"`, k)
	vw, err := wc.dw.WriteDocumentElement(qk)
	if err != nil {
		return err
	}

	if op == "=" {
		return enc.encodeValue(wc.ec, v, vt, vw)
	} else {
		dw, err := vw.WriteDocument()
		if err != nil {
			return err
		}

		k = opKey(op)
		wc.dw = dw

		err = enc.encodeElement(wc, k, v, vt, "=")
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

func (enc NodeEncoder) encodeValue(ec bsoncodec.EncodeContext, v []byte, vt Token, vw bsonrw.ValueWriter) error {
	if string(v) == "null" {
		return vw.WriteNull()
	}

	rv := restoreValue(v, vt)
	lv := enc.lookupValue(rv)

	encoder, err := enc.lookupEncoder(ec, reflect.TypeOf(lv))
	if err != nil {
		return err
	}

	err = encoder.EncodeValue(ec, vw, reflect.ValueOf(lv))
	if err != nil {
		return err
	}
	return nil
}

func (enc NodeEncoder) lookupValue(v interface{}) interface{} {
	s, ok := v.(string)
	if ok && strings.HasPrefix(s, "$") {
		pv, ok := enc.prmMap[s]
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

func (enc NodeEncoder) lookupEncoder(ec bsoncodec.EncodeContext, typ reflect.Type) (bsoncodec.ValueEncoder, error) {
	if typ.ConvertibleTo(tD) {
		return nil, nil
	} else if typ.ConvertibleTo(tA) {
		return nil, nil
	}

	return ec.LookupEncoder(typ)
}
