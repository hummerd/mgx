package msql_test

import (
	"strings"
	"testing"

	"github.com/hummerd/mgx/msql"
	"github.com/stretchr/testify/assert"
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
					R:  []byte{0, 0, 0, 0, 0, 0, 0, 90},
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
					R:  []byte("\"90\""),
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
					R:  []byte("\"90\""),
				},
				R: &msql.Expression{
					Op: "=",
					L:  []byte("\"don\""),
					R:  []byte("d"),
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
						R:  []byte("\"90\""),
					},
					R: &msql.Expression{
						Op: "=",
						L:  []byte("\"don\""),
						R:  []byte("d"),
					},
				},
				R: &msql.Expression{
					Op: "=",
					L:  []byte("c"),
					R:  []byte("e"),
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

			assert.Equal(t, got, tt.want)
		})
	}
}
