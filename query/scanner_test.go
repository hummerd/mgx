package query_test

import (
	"strings"
	"testing"

	"github.com/hummerd/mgx/query"
)

func TestScanner(t *testing.T) {
	src := `a > 75 AND (d OR c)   AND b < 4 AND
		"abc" = 90 AND g $regex /abc/ig and a = 'some' OR
		arr $in ["a", 18, ISODate('2022-01-01T00:00:00Z')] and
		f = 0.15`

	exp := []string{
		"a", ">", "75", "AND", "(", "d", "OR", "c", ")",
		"AND", "b", "<", "4", "AND", "\"abc\"", "=", "90",
		"AND", "g", "$regex", "/abc/ig", "and", "a", "=", `'some'`,
		"OR", "arr", "$in", "[", `"a"`, ",", "18", ",", "ISODate", "(", "'2022-01-01T00:00:00Z'", ")", "]",
		"and", "f", "=", "0.15",
	}

	s := query.NewScanner(strings.NewReader(src))

	i := 0
	for s.Next() == nil {
		tok, l := s.Token()
		if tok == 0 {
			t.Fatal("unexpected token")
		}

		if string(l) != exp[i] {
			t.Fatalf("unexpected literal got: '%s'; expected: '%s'", string(l), exp[i])
		}
		i++
	}

	if i < len(exp) {
		t.Fatal("not all tokens read", i, len(exp))
	}

	l, c := s.Position()
	if l != 4 || c != 11 {
		t.Fatal("unexpected position", l, c)
	}
}
