package query_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hummerd/mgx/query"
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
				{Key: `a`, Value: int64(90)},
			},
		},
		{
			name:  "simple string",
			query: `a.c > "abc"`,
			want: &bson.D{
				{Key: `a.c`, Value: bson.D{{Key: `$gt`, Value: `abc`}}},
			},
		},
		{
			name:  "simple and",
			query: `a.c < "abc" and e = 90`,
			want: &bson.D{
				{Key: `a.c`, Value: bson.D{{Key: `$lt`, Value: `abc`}}},
				{Key: `e`, Value: int64(90)},
			},
		},
		{
			name:  "simple or",
			query: `a.c >= "abc" or e = 90`,
			want: &bson.D{
				{Key: "$or", Value: bson.A{
					bson.D{{Key: `a.c`, Value: bson.D{{Key: `$gte`, Value: `abc`}}}},
					bson.D{{Key: `e`, Value: int64(90)}},
				}},
			},
		},
		{
			name:  "and or",
			query: `a.c > "abc" and f = "some" or e = 90`,
			want: &bson.D{
				{Key: "$or", Value: bson.A{
					bson.D{
						{Key: `a.c`, Value: bson.D{{Key: `$gt`, Value: `abc`}}},
						{Key: `f`, Value: `some`},
					},
					bson.D{{Key: `e`, Value: int64(90)}},
				}},
			},
		},
		{
			name:  "and or or",
			query: `a.c <= "abc" and f = "some" or e = 90 or g = 100`,
			want: &bson.D{
				{Key: "$or", Value: bson.A{
					bson.D{
						{Key: `a.c`, Value: bson.D{{Key: `$lte`, Value: `abc`}}},
						{Key: `f`, Value: `some`},
					},
					bson.D{{Key: `e`, Value: int64(90)}},
					bson.D{{Key: `g`, Value: int64(100)}},
				}},
			},
		},

		{
			name:  "and or with brackets",
			query: `a.c > "abc" and (f = "some" or e = 90)`,
			want: &bson.D{
				{Key: `a.c`, Value: bson.D{{Key: `$gt`, Value: `abc`}}},
				{
					Key: "$or", Value: bson.A{
						bson.D{{Key: `f`, Value: `some`}},
						bson.D{{Key: `e`, Value: int64(90)}},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cq, err := query.Compile(tt.query)
			if err != nil {
				t.Fatal(err)
			}

			mq, err := cq.MarshalBSON()
			if err != nil {
				t.Fatal(err)
			}

			expectedQuery, err := bson.Marshal(tt.want)
			if err != nil {
				t.Fatal(err)
			}

			printMarshalled(t, mq)

			if !reflect.DeepEqual(expectedQuery, mq) {
				t.Errorf("CompileToBSON() = %s, want %s",
					bson.Raw(mq),
					bson.Raw(expectedQuery))
			}
		})
	}
}

func printMarshalled(t *testing.T, marshalledQuery []byte) {
	var q interface{}

	err := bson.Unmarshal(marshalledQuery, &q)
	if err != nil {
		t.Fatal(err)
	}

	j, _ := bson.MarshalExtJSONIndent(q, false, true, "", " ")
	t.Log(string(j))
}

func TestEncoder_ConcurrentEncode(t *testing.T) {
	pq, err := query.Prepare(`a >= "$prm"`)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errs := [4]error{}

	expNumBson := bson.D{
		{Key: "a", Value: bson.D{{Key: "$gte", Value: 20}}},
	}
	expNumBytes, err := bson.Marshal(expNumBson)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 2; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			for {
				if ctx.Err() != nil {
					return
				}

				q, err := pq.Compile("$prm", 20)
				if err != nil {
					errs[i] = err
					return
				}

				qb, err := q.MarshalBSON()
				if err != nil {
					errs[i] = err
					return
				}

				if !reflect.DeepEqual(qb, expNumBytes) {
					errs[i] = fmt.Errorf("unexpected num bson")
					return
				}
			}
		}(i)
	}

	testDate := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)

	expTimeBson := bson.D{
		{Key: "a", Value: bson.D{{Key: "$gte", Value: testDate}}},
	}
	expTimeBytes, err := bson.Marshal(expTimeBson)
	if err != nil {
		t.Fatal(err)
	}

	for i := 2; i < 4; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			for {
				if ctx.Err() != nil {
					return
				}

				q, err := pq.Compile("$prm", testDate)
				if err != nil {
					errs[i] = err
					return
				}

				qb, err := q.MarshalBSON()
				if err != nil {
					errs[i] = err
					return
				}

				if !reflect.DeepEqual(qb, expTimeBytes) {
					errs[i] = fmt.Errorf("unexpected time bson")
					return
				}
			}
		}(i)
	}

	time.Sleep(5 * time.Second)
	cancel()

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
