package mgx_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hummerd/mgx"
	"go.mongodb.org/mongo-driver/bson"
)

func TestMustParseQuery(t *testing.T) {
	t.Parallel()

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
			want: bson.A{
				bson.D{{Key: "$match", Value: bson.D{{Key: "id", Value: "abc"}}}},
				bson.D{{Key: "$limit", Value: 12}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mgx.MustParseQuery(tt.args.query, tt.args.keyValues...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MustParseQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMustParseQueryCachedRace(t *testing.T) {
	t.Parallel()

	ts := time.Now()

	query := `{
		"id" : "$1",
		"start": { "$lte": "$2" },
		"$or": [
			{ "end": { "$exists": false } },
			{ "end": null },
			{ "end": "$$$" },
			{ "end": { "$gte": "$2" } }
		]}`

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(3)

	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()

			for ctx.Err() == nil {
				cc := mgx.MustParseQuery(query,
					"$1", "abc",
					"$2", ts)
				t.Log(cc)
				time.Sleep(time.Millisecond * 100)
			}
		}()
	}

	wg.Wait()
}
