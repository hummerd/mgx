package msql_test

import (
	"reflect"
	"testing"

	"github.com/hummerd/mgx/msql"
	"go.mongodb.org/mongo-driver/bson"
)

func TestCompileToBSON(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    interface{}
		wantErr bool
	}{
		{
			name:  "simple number",
			query: "a = 90",
			want: &bson.D{
				{Key: `"a"`, Value: int64(90)},
			},
		},
		{
			name:  "simple number",
			query: `a.c > "abc"`,
			want: &bson.D{
				{Key: `"a.c"`, Value: bson.D{{Key: `"$gt"`, Value: `abc`}}},
			},
		},
		{
			name:  "simple and",
			query: `a.c > "abc" and e = 90`,
			want: &bson.D{
				{Key: `"a.c"`, Value: bson.D{{Key: `"$gt"`, Value: `abc`}}},
				{Key: `"e"`, Value: int64(90)},
			},
		},
		{
			name:  "simple or",
			query: `a.c > "abc" or e = 90`,
			want: &bson.D{
				{Key: "$or", Value: bson.A{
					bson.D{{Key: `"a.c"`, Value: bson.D{{Key: `"$gt"`, Value: `abc`}}}},
					bson.D{{Key: `"e"`, Value: int64(90)}},
				}},
			},
		},
		{
			name:  "and or",
			query: `a.c > "abc" and f = "some" or e = 90`,
			want: &bson.D{
				{Key: "$or", Value: bson.A{
					bson.D{
						{Key: `"a.c"`, Value: bson.D{{Key: `"$gt"`, Value: `abc`}}},
						{Key: `"f"`, Value: `some`},
					},
					bson.D{{Key: `"e"`, Value: int64(90)}},
				}},
			},
		},
		{
			name:  "and or with brackets",
			query: `a.c > "abc" and (f = "some" or e = 90)`,
			want: &bson.D{
				{Key: `"a.c"`, Value: bson.D{{Key: `"$gt"`, Value: `abc`}}},
				{
					Key: "$or", Value: bson.A{
						bson.D{{Key: `"f"`, Value: `some`}},
						bson.D{{Key: `"e"`, Value: int64(90)}},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mq, err := msql.CompileToBSON(tt.query, nil)
			if err != nil {
				t.Fatal(err)
			}

			marshalledQuery, _ := mq.MarshalBSON()

			expectedQuery, err := bson.Marshal(tt.want)
			if err != nil {
				t.Fatal(err)
			}

			printMarshalled(t, marshalledQuery)

			if !reflect.DeepEqual(expectedQuery, marshalledQuery) {
				t.Errorf("CompileToBSON() = %v, want %v", marshalledQuery, expectedQuery)
			}
		})
	}
}

func printMarshalled(t *testing.T, marshalledQuery []byte) {
	var q interface{}
	bson.Unmarshal(marshalledQuery, &q)

	j, _ := bson.MarshalExtJSONIndent(q, false, true, "", " ")
	t.Log(string(j))
}
