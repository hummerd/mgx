package query_test

import (
	"context"
	"flag"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hummerd/mgx/query"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mgxApp        = "mgx-test"
	mgxDB         = "mgx"
	mgxCollection = "items"
)

type item struct {
	Name  string `bson:"name"`
	Num   int    `bson:"num"`
	Child *item  `bson:"child,omitempty"`
}

var testItems = []item{
	{
		Name: "item1",
		Num:  1,
	},
	{
		Name: "item2",
		Num:  2,
	},
	{
		Name: "item3",
		Num:  3,
		Child: &item{
			Name: "item31",
			Num:  31,
		},
	},
}

var coll *mongo.Collection

func TestMain(m *testing.M) {
	if !flag.Parsed() {
		flag.Parse()
	}

	var client *mongo.Client

	mongoURI := os.Getenv("MGX_INTEGRATION_MONGO_URI")
	// mongoURI := "mongodb://admin:password@localhost:27017"
	if mongoURI != "" {
		client = mongoClient(mongoURI)
		coll = client.Database(mgxDB).Collection(mgxCollection)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		_ = coll.Drop(ctx)
		addTestItems(ctx, coll)
		cancel()
	}

	ec := m.Run()

	if client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		_ = client.Disconnect(ctx)
		cancel()
	}

	os.Exit(ec)
}

func TestDB_FindOne(t *testing.T) {
	if coll == nil {
		t.Skip("integration mode disabled")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	mq, err := query.CompileToBSON(`name = "item1"`, nil)
	if err != nil {
		t.Fatal(err)
	}

	var it item

	err = coll.FindOne(ctx, mq).Decode(&it)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(it, testItems[0]) {
		t.Fatal("wrong items", it, testItems[0])
	}
}

func TestDB_FindMany(t *testing.T) {
	if coll == nil {
		t.Skip("integration mode disabled")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	mq, err := query.CompileToBSON(`num >= 2`, nil)
	if err != nil {
		t.Fatal(err)
	}

	curr, err := coll.Find(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}

	var items []item
	err = curr.All(ctx, &items)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(items, testItems[1:]) {
		t.Fatal("wrong items", items, testItems[1:])
	}
}

func mongoClient(uri string) *mongo.Client {
	opt := options.Client().
		ApplyURI(uri).
		SetAppName(mgxApp)
	client, err := mongo.NewClient(opt)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		panic(err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		panic(err)
	}

	return client
}

func addTestItems(ctx context.Context, coll *mongo.Collection) {
	result := make([]interface{}, len(testItems))

	for i, v := range testItems {
		result[i] = v
	}

	_, err := coll.InsertMany(ctx, result)
	if err != nil {
		panic(err)
	}
}
