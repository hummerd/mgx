package mgx

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
)

var (
	queryCache = make(map[string]interface{})
	queryLock  = sync.RWMutex{}
)

// MustParseQuery creates bson request from specified JSON and parameters. Same as ParseQuery
// but panics if there was an error during parsing JSON..
func MustParseQuery(query string, keyValues ...interface{}) interface{} {
	i, err := ParseQuery(query, keyValues...)
	if err != nil {
		panic(fmt.Sprintf("can not compile query: \"%s\", error: %v", query, err))
	}

	return i
}

// ParseQuery creates bson document from specified JSON and specified parameters.ParseQuery uses
// internal cache so query will be decoded only once, and then cached values will be used.
func ParseQuery(query string, keyValues ...interface{}) (interface{}, error) {
	queryLock.RLock()
	cq, ok := queryCache[query]
	queryLock.RUnlock()

	if ok {
		cq = clone(cq)
		err := setParams(cq, keyValues...)
		return cq, err
	}

	queryLock.Lock()
	defer queryLock.Unlock()

	// double check resource locking
	cq, ok = queryCache[query]
	if ok {
		cq = clone(cq)
		err := setParams(cq, keyValues...)
		return cq, err
	}

	var i interface{}

	err := bson.UnmarshalExtJSON([]byte(query), false, &i)
	if err != nil {
		return nil, err
	}

	queryCache[query] = i

	i = clone(i)
	err = setParams(i, keyValues...)
	return i, err
}

func makeParamMap(keyValues ...interface{}) (map[string]interface{}, error) {
	if len(keyValues) == 0 {
		return nil, nil
	}

	if len(keyValues)%2 != 0 {
		return nil, errors.New("keyValues should be pairs of string key and any value")
	}

	prmMap := make(map[string]interface{}, len(keyValues)/2)

	for i := 0; i < len(keyValues); i += 2 {
		s, ok := keyValues[i].(string)
		if !ok {
			return nil, fmt.Errorf("parameter key %v must be string", keyValues[i])
		}

		prmMap[s] = keyValues[i+1]
	}

	return prmMap, nil
}

func setParams(query interface{}, keyValues ...interface{}) error {
	prmMap, err := makeParamMap(keyValues...)
	if err != nil {
		return err
	}

	traverse(query, prmMap)
	return nil
}

func traverse(node interface{}, prmMap map[string]interface{}) {
	switch v := node.(type) {
	case bson.D:
		for i, e := range v {
			s, ok := e.Value.(string)
			if ok && strings.HasPrefix(s, "$") {
				pv, ok := prmMap[s]
				if !ok {
					continue
				}

				v[i] = bson.E{Key: e.Key, Value: pv}

				continue
			}

			traverse(e.Value, prmMap)
		}
	case bson.A:
		for i, e := range v {
			s, ok := e.(string)
			if ok && strings.HasPrefix(s, "$") {
				pv, ok := prmMap[s]
				if !ok {
					continue
				}

				v[i] = pv

				continue
			}

			traverse(e, prmMap)
		}
	}
}

func clone(node interface{}) interface{} {
	switch v := node.(type) {
	case bson.D:
		c := make(bson.D, 0, len(v))
		for _, e := range v {
			c = append(c, bson.E{Key: e.Key, Value: clone(e.Value)})
		}
		return c
	case bson.A:
		a := make(bson.A, 0, len(v))
		for _, e := range v {
			a = append(a, clone(e))
		}
		return a
	}

	return node
}
