package mgx

import (
	"bytes"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
)

var buffPool = sync.Pool{New: func() interface{} {
	return &bytes.Buffer{}
}}

var defaultRegistry = bson.NewRegistry()

func NewMarshalledQuery(bsonData *bytes.Buffer) MarshalledQuery {
	return MarshalledQuery{
		bsonData: bsonData,
	}
}

type MarshalledQuery struct {
	bsonData *bytes.Buffer
}

// MarshalBSON just returns marshalled bson document.
func (q MarshalledQuery) MarshalBSON() ([]byte, error) {
	return q.bsonData.Bytes(), nil
}

// MarshalBSONValue returns marshalled bson document as bson value.
// We use bson array here because it is the only type that can be used as pipeline.
func (q MarshalledQuery) MarshalBSONValue() (bson.Type, []byte, error) {
	return bson.TypeArray, q.bsonData.Bytes(), nil
}

// Close returns marshal buffer to internal pool and returns nil.
func (q MarshalledQuery) Close() {
	q.bsonData.Reset()
	buffPool.Put(q.bsonData)
}

// MarshalQuery creates serialized representation of query or pipeline.
func MarshalQuery(query interface{}, params ...interface{}) (MarshalledQuery, error) {
	prmMap, err := makeParamMap(params...)
	if err != nil {
		return MarshalledQuery{}, err
	}

	enc := NewQueryEncoder(prmMap)

	ec := bson.EncodeContext{Registry: defaultRegistry}

	buff := buffPool.Get().(*bytes.Buffer)

	vw := bson.NewDocumentWriter(buff)

	val := reflect.ValueOf(query)

	if val.Type().ConvertibleTo(tA) {
		err = enc.EncodePipeline(ec, vw, val)
	} else {
		err = enc.EncodeValue(ec, vw, val)
	}

	return MarshalledQuery{buff}, err
}

func NewQueryEncoder(params map[string]interface{}) QueryEncoder {
	return QueryEncoder{
		prmMap: params,
	}
}

var (
	tString = reflect.TypeOf(string(""))
	tD      = reflect.TypeOf(bson.D{})
	tA      = reflect.TypeOf(bson.A{})
)

type QueryEncoder struct {
	prmMap map[string]interface{}
}

func (enc QueryEncoder) EncodePipeline(ec bson.EncodeContext, vw bson.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Kind() != reflect.Slice {
		return bson.ValueEncoderError{Name: "EncodeValue", Kinds: []reflect.Kind{reflect.Slice}, Received: val}
	}

	dw, err := vw.WriteDocument()
	if err != nil {
		return err
	}

	for idx := 0; idx < val.Len(); idx++ {
		currVal := val.Index(idx)

		if currVal.Kind() == reflect.Interface {
			currVal = currVal.Elem()
		}

		vw, err := dw.WriteDocumentElement(strconv.Itoa(idx))
		if err != nil {
			return err
		}

		err = enc.writeDDocument(ec, vw, currVal)
		if err != nil {
			return err
		}
	}

	return dw.WriteDocumentEnd()
}

func (enc QueryEncoder) EncodeValue(ec bson.EncodeContext, vw bson.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Kind() != reflect.Slice {
		return bson.ValueEncoderError{Name: "EncodeValue", Kinds: []reflect.Kind{reflect.Slice}, Received: val}
	}

	if val.IsNil() {
		return vw.WriteNull()
	}

	elemType := val.Type().Elem()

	if val.Type().ConvertibleTo(tD) {
		return enc.writeDDocument(ec, vw, val)
	}

	return enc.writeArray(ec, vw, elemType, val)
}

func (enc QueryEncoder) writeDDocument(ec bson.EncodeContext, vw bson.ValueWriter, val reflect.Value) error {
	d := val.Convert(tD).Interface().(bson.D)

	dw, err := vw.WriteDocument()
	if err != nil {
		return err
	}

	for _, e := range d {
		err = enc.encodeElement(ec, dw, e)
		if err != nil {
			return err
		}
	}

	return dw.WriteDocumentEnd()
}

func (enc QueryEncoder) writeArray(ec bson.EncodeContext, vw bson.ValueWriter, elemType reflect.Type, val reflect.Value) error {
	aw, err := vw.WriteArray()
	if err != nil {
		return err
	}

	var encoder bson.ValueEncoder
	var prevType reflect.Type

	for idx := 0; idx < val.Len(); idx++ {
		currVal := val.Index(idx)
		if currVal.Kind() == reflect.Interface {
			currVal = currVal.Elem()
		}

		currVal = enc.lookupReflectValue(currVal)

		vw, err := aw.WriteArrayElement()
		if err != nil {
			return err
		}

		if !currVal.IsValid() {
			err = vw.WriteNull()
			if err != nil {
				return err
			}
			continue
		}

		currType := currVal.Type()
		if encoder == nil || prevType == nil || prevType != currType {
			encoder, err = enc.lookupEncoder(ec, currType)
			if err != nil {
				return err
			}

			prevType = currType
		}

		err = encoder.EncodeValue(ec, vw, currVal)
		if err != nil {
			return err
		}
	}

	return aw.WriteArrayEnd()
}

func (enc QueryEncoder) encodeElement(ec bson.EncodeContext, dw bson.DocumentWriter, e bson.E) error {
	vw, err := dw.WriteDocumentElement(e.Key)
	if err != nil {
		return err
	}

	if e.Value == nil {
		return vw.WriteNull()
	}

	v := enc.lookupValue(e.Value)

	encoder, err := enc.lookupEncoder(ec, reflect.TypeOf(v))
	if err != nil {
		return err
	}

	err = encoder.EncodeValue(ec, vw, reflect.ValueOf(v))
	if err != nil {
		return err
	}
	return nil
}

func (enc QueryEncoder) lookupValue(v interface{}) interface{} {
	s, ok := v.(string)
	if ok && strings.HasPrefix(s, "$") {
		pv, ok := enc.prmMap[s]
		if ok {
			return pv
		}
	}

	return v
}

func (enc QueryEncoder) lookupReflectValue(val reflect.Value) reflect.Value {
	if !val.CanConvert(tString) {
		return val
	}

	s, ok := val.Interface().(string)
	if ok && strings.HasPrefix(s, "$") {
		pv, ok := enc.prmMap[s]
		if ok {
			return reflect.ValueOf(pv)
		}
	}

	return val
}

func (enc QueryEncoder) lookupEncoder(ec bson.EncodeContext, typ reflect.Type) (bson.ValueEncoder, error) {
	if typ.ConvertibleTo(tD) {
		return enc, nil
	} else if typ.ConvertibleTo(tA) {
		return enc, nil
	}

	return ec.LookupEncoder(typ)
}
