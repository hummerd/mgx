package mgx_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/mgx"
	"go.mongodb.org/mongo-driver/bson"
)

func TestMarshalQuery(t *testing.T) {
	ts := time.Now()

	type args struct {
		query     string
		keyValues []interface{}
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "simple",
			args: args{
				query: `{
					"a": "$1"
				}`,
			},
			want: bson.D{
				{Key: "a", Value: "$1"},
			},
		},
		{
			name: "simple",
			args: args{
				query: `{
					"_id": ["$1", "$2"]
				}`,
				keyValues: []interface{}{
					"$1", "a",
					"$2", "b",
				},
			},
			want: bson.D{
				{Key: "_id", Value: bson.A{"a", "b"}},
			},
		},
		{
			name: "complex",
			args: args{
				query: `{
					"id" : "$1",
					"start": { "$lte": "$2" },
					"$or": [
						{ "end": { "$exists": false } },
						{ "end": null },
						{ "end": "$$$" },
						{ "end": { "$gte": "$2" } }
					]}`,
				keyValues: []interface{}{
					"$1", "abc",
					"$2", ts,
				},
			},
			want: bson.D{
				{Key: "id", Value: "abc"},
				{Key: "start", Value: bson.D{{Key: "$lte", Value: ts}}},
				{Key: "$or", Value: bson.A{
					bson.D{{Key: "end", Value: bson.D{{Key: "$exists", Value: false}}}},
					bson.D{{Key: "end", Value: nil}},
					bson.D{{Key: "end", Value: "$$$"}},
					bson.D{{Key: "end", Value: bson.D{{Key: "$gte", Value: ts}}}},
				}},
			},
		},
		{
			name: "pipeline",
			args: args{
				query: `[{
					"$match": { "id": "$1" }
				},
				{
					"$limit": "$2"
				}]`,
				keyValues: []interface{}{
					"$1", "abc",
					"$2", 12,
				},
			},
			want: bson.D{
				{Key: "0", Value: bson.D{{Key: "$match", Value: bson.D{{Key: "id", Value: "abc"}}}}},
				{Key: "1", Value: bson.D{{Key: "$limit", Value: 12}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq, err := mgx.ParseQuery(tt.args.query)
			if err != nil {
				t.Fatal(err)
			}

			mq, err := mgx.MarshalQuery(pq, tt.args.keyValues...)
			if err != nil {
				t.Fatal(err)
			}

			marshalledQuery, _ := mq.MarshalBSON()

			expectedQuery, err := bson.Marshal(tt.want)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(expectedQuery, marshalledQuery) {
				t.Errorf("MarshalQuery() = %v, want %v", marshalledQuery, expectedQuery)
			}
		})
	}
}
