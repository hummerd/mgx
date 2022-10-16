package query_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hummerd/mgx/query"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestParser_Parse(t *testing.T) {
	testTime := time.Date(2022, 1, 1, 0, 0, 0, 200*1000000, time.UTC)
	btestTime := binary.BigEndian.AppendUint64(nil, uint64(primitive.NewDateTimeFromTime(testTime)))

	testTimeNoMs := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	btestTimeNoMs := binary.BigEndian.AppendUint64(nil, uint64(primitive.NewDateTimeFromTime(testTimeNoMs)))

	testOid, _ := primitive.ObjectIDFromHex("507f191e810c19729de860ea")
	btestOid := testOid[:]

	tests := []struct {
		name       string
		expression string
		want       *query.Node
		wantErr    bool
	}{
		{
			name:       "simple number",
			expression: "a > 90",
			want: &query.Node{
				Op: "and",
				L: &query.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: query.VTKey,
					R:  []byte{0, 0, 0, 0, 0, 0, 0, 90},
					RT: query.VTNumber,
				},
			},
		},
		{
			name:       "simple string",
			expression: "a > \"90\"",
			want: &query.Node{
				Op: "and",
				L: &query.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: query.VTKey,
					R:  []byte("\"90\""),
					RT: query.VTString,
				},
			},
		},
		{
			name:       "simple string (single quote)",
			expression: "a > '90'",
			want: &query.Node{
				Op: "and",
				L: &query.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: query.VTKey,
					R:  []byte("'90'"),
					RT: query.VTString,
				},
			},
		},
		{
			name:       "simple date time",
			expression: `a > ISODate("2022-01-01T00:00:00.200Z")`,
			want: &query.Node{
				Op: "and",
				L: &query.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: query.VTKey,
					R:  btestTime,
					RT: query.VTDate,
				},
			},
		},
		{
			name:       "simple date time (no ms)",
			expression: `a > ISODate("2022-01-01T00:00:00Z")`,
			want: &query.Node{
				Op: "and",
				L: &query.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: query.VTKey,
					R:  btestTimeNoMs,
					RT: query.VTDate,
				},
			},
		},
		{
			name:       "simple object id",
			expression: `a = ObjectId("507f191e810c19729de860ea")`,
			want: &query.Node{
				Op: "and",
				L: &query.Expression{
					Op: "=",
					L:  []byte("a"),
					LT: query.VTKey,
					R:  btestOid,
					RT: query.VTObjectID,
				},
			},
		},
		{
			name:       "simple and",
			expression: "a > \"90\" and \"don\" = d",
			want: &query.Node{
				Op: "and",
				L: &query.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: query.VTKey,
					R:  []byte("\"90\""),
					RT: query.VTString,
				},
				R: &query.Expression{
					Op: "=",
					L:  []byte("\"don\""),
					LT: query.VTString,
					R:  []byte("d"),
					RT: query.VTKey,
				},
			},
		},
		{
			name:       "simple and or",
			expression: `a > "90" and "don" = d or c = e`,
			want: &query.Node{
				Op: "or",
				LN: &query.Node{
					Op: "and",
					L: &query.Expression{
						Op: ">",
						L:  []byte("a"),
						LT: query.VTKey,
						R:  []byte("\"90\""),
						RT: query.VTString,
					},
					R: &query.Expression{
						Op: "=",
						L:  []byte("\"don\""),
						LT: query.VTString,
						R:  []byte("d"),
						RT: query.VTKey,
					},
				},
				R: &query.Expression{
					Op: "=",
					L:  []byte("c"),
					LT: query.VTKey,
					R:  []byte("e"),
					RT: query.VTKey,
				},
			},
		},
		{
			name:       "simple and or with brackets",
			expression: `a > "90" and ("don" = d or c = e)`,
			want: &query.Node{
				Op: "and",
				L: &query.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: query.VTKey,
					R:  []byte("\"90\""),
					RT: query.VTString,
				},
				RN: &query.Node{
					Op: "or",
					L: &query.Expression{
						Op: "=",
						L:  []byte("\"don\""),
						LT: query.VTString,
						R:  []byte("d"),
						RT: query.VTKey,
					},
					R: &query.Expression{
						Op: "=",
						L:  []byte("c"),
						LT: query.VTKey,
						R:  []byte("e"),
						RT: query.VTKey,
					},
				},
			},
		},
		{
			name:       "simple and or with brackets",
			expression: `(a > "90" and "don" = d) or c = e`,
			want: &query.Node{
				Op: "or",
				LN: &query.Node{
					Op: "and",
					L: &query.Expression{
						Op: ">",
						L:  []byte("a"),
						LT: query.VTKey,
						R:  []byte("\"90\""),
						RT: query.VTString,
					},
					R: &query.Expression{
						Op: "=",
						L:  []byte("\"don\""),
						LT: query.VTString,
						R:  []byte("d"),
						RT: query.VTKey,
					},
				},
				R: &query.Expression{
					Op: "=",
					L:  []byte("c"),
					LT: query.VTKey,
					R:  []byte("e"),
					RT: query.VTKey,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := query.NewScanner(strings.NewReader(tt.expression))
			p := query.NewParser(s)

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

func TestParser_ParseAndLink(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       *query.Node
		wantErr    bool
	}{
		{
			name:       "simple link",
			expression: "a > 90 and a < 100",
			want: &query.Node{
				Op: "and",
				L: &query.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: query.VTKey,
					R:  []byte{0, 0, 0, 0, 0, 0, 0, 90},
					RT: query.VTNumber,
					Links: &[]*query.Expression{
						{
							Op: "<",
							L:  []byte("a"),
							LT: query.VTKey,
							R:  []byte{0, 0, 0, 0, 0, 0, 0, 100},
							RT: query.VTNumber,
						},
					},
				},
			},
		},
		{
			name:       "link in brackets",
			expression: "(a > 90 and a < 100) or a = 25",
			want: &query.Node{
				Op: "or",
				L: &query.Expression{
					Op: ">",
					L:  []byte("a"),
					LT: query.VTKey,
					R:  []byte{0, 0, 0, 0, 0, 0, 0, 90},
					RT: query.VTNumber,
					Links: &[]*query.Expression{
						{
							Op: "<",
							L:  []byte("a"),
							LT: query.VTKey,
							R:  []byte{0, 0, 0, 0, 0, 0, 0, 100},
							RT: query.VTNumber,
						},
					},
				},
				R: &query.Expression{
					Op: "=",
					L:  []byte("a"),
					LT: query.VTKey,
					R:  []byte{0, 0, 0, 0, 0, 0, 0, 25},
					RT: query.VTNumber,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := query.NewScanner(strings.NewReader(tt.expression))
			p := query.NewParser(s)

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

func compareNodes(a, b *query.Node) error {
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

func compareExpressions(a, b *query.Expression) error {
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
		return fmt.Errorf("left lexeme mismatch %s with %s", a, b)
	}

	if !bytes.Equal(a.R, b.R) {
		return fmt.Errorf("right lexeme mismatch %s with %s", a, b)
	}

	if (a.Links == nil) != (b.Links == nil) {
		return fmt.Errorf("links nil not nil %s - %s", a, b)
	}

	if (a.Links != nil) && (b.Links != nil) {
		if len(*a.Links) != len(*b.Links) {
			return fmt.Errorf("links not equal %s - %s", a, b)
		}

		for i := range *a.Links {
			err := compareExpressions((*a.Links)[i], (*b.Links)[i])
			if err != nil {
				return fmt.Errorf("links not equal %s - %s: %w", a, b, err)
			}
		}
	}

	return nil
}
