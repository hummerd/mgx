package msql_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/hummerd/mgx/msql"
)

func TestParser_Parse(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       *msql.Node
		wantErr    bool
	}{
		{
			name:       "simple number",
			expression: "a > 90",
			want: &msql.Node{
				Op: "and",
				L: &msql.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: msql.TKey,
					R:  []byte{0, 0, 0, 0, 0, 0, 0, 90},
					RT: msql.TNumber,
				},
			},
		},
		{
			name:       "simple string",
			expression: "a > \"90\"",
			want: &msql.Node{
				Op: "and",
				L: &msql.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: msql.TKey,
					R:  []byte("\"90\""),
					RT: msql.TString,
				},
			},
		},
		{
			name:       "simple and",
			expression: "a > \"90\" and \"don\" = d",
			want: &msql.Node{
				Op: "and",
				L: &msql.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: msql.TKey,
					R:  []byte("\"90\""),
					RT: msql.TString,
				},
				R: &msql.Expression{
					Op: "=",
					L:  []byte("\"don\""),
					LT: msql.TString,
					R:  []byte("d"),
					RT: msql.TKey,
				},
			},
		},
		{
			name:       "simple and or",
			expression: `a > "90" and "don" = d or c = e`,
			want: &msql.Node{
				Op: "or",
				LN: &msql.Node{
					Op: "and",
					L: &msql.Expression{
						Op: ">",
						L:  []byte("a"),
						LT: msql.TKey,
						R:  []byte("\"90\""),
						RT: msql.TString,
					},
					R: &msql.Expression{
						Op: "=",
						L:  []byte("\"don\""),
						LT: msql.TString,
						R:  []byte("d"),
						RT: msql.TKey,
					},
				},
				R: &msql.Expression{
					Op: "=",
					L:  []byte("c"),
					LT: msql.TKey,
					R:  []byte("e"),
					RT: msql.TKey,
				},
			},
		},
		{
			name:       "simple and or with brackets",
			expression: `a > "90" and ("don" = d or c = e)`,
			want: &msql.Node{
				Op: "and",
				L: &msql.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: msql.TKey,
					R:  []byte("\"90\""),
					RT: msql.TString,
				},
				RN: &msql.Node{
					Op: "or",
					L: &msql.Expression{
						Op: "=",
						L:  []byte("\"don\""),
						LT: msql.TString,
						R:  []byte("d"),
						RT: msql.TKey,
					},
					R: &msql.Expression{
						Op: "=",
						L:  []byte("c"),
						LT: msql.TKey,
						R:  []byte("e"),
						RT: msql.TKey,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := msql.NewScanner(strings.NewReader(tt.expression))
			p := msql.NewParser(s)

			got, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parser.Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			tt.want.FixParent()
			err = compareNodes(tt.want, got)
			if err != nil {
				t.Log("want", tt.want.String())
				t.Log("got", got.String())
				t.Error("Parser.Parse() unexpected result", err)
			}
		})
	}
}

func compareNodes(a, b *msql.Node) error {
	if a == nil && b == nil {
		return nil
	}

	if a == nil || b == nil {
		return fmt.Errorf("nil not nil %s with %s", a, b)
	}

	if a.Op != b.Op {
		return fmt.Errorf("operation mismatch %s with %s", a, b)
	}

	err := compareExpressions(a.L, b.L)
	if err != nil {
		return fmt.Errorf("left expression mismatch %s with %s: %w", a, b, err)
	}

	err = compareExpressions(a.R, b.R)
	if err != nil {
		return fmt.Errorf("left expression mismatch %s with %s: %w", a, b, err)
	}

	err = compareNodes(a.LN, b.LN)
	if err != nil {
		return fmt.Errorf("left node mismatch %s with %s: %w", a, b, err)
	}

	err = compareNodes(a.RN, b.RN)
	if err != nil {
		return fmt.Errorf("right node mismatch %s with %s: %w", a, b, err)
	}

	return nil
}

func compareExpressions(a, b *msql.Expression) error {
	if a == nil && b == nil {
		return nil
	}

	if a == nil || b == nil {
		return fmt.Errorf("nil not nil %s - %s", a, b)
	}

	if a.Op != b.Op {
		return fmt.Errorf("operation mismatch %s with %s", a, b)
	}

	if a.LT != b.LT {
		return fmt.Errorf("left token mismatch %s with %s", a, b)
	}

	if a.RT != b.RT {
		return fmt.Errorf("right token mismatch %s with %s", a, b)
	}

	if !bytes.Equal(a.L, b.L) {
		return fmt.Errorf("left lexem mismatch %s with %s", a, b)
	}

	if !bytes.Equal(a.R, b.R) {
		return fmt.Errorf("right lexeme mismatch %s with %s", a, b)
	}

	return nil
}
