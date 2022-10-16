# mgx
Extension for Golang MongoDB driver. 

It allows you to work with quries and pipelines in form of JSON, because it is easier to read and you can copy and paste it between Mongo Atlas/Compass/MongoSH.

## Usage

``` GO
filter := mgx.MustParseQuery(`{
        "id" : "$1",
        "start": { "$lte": "$2" },
        "$or": [
            { "end": { "$exists": false } },
            { "end": null },
            { "end": { "$gte": "$2" } }
        ]}`,
    "$1", id,
    "$2", date,
)
```
or

``` GO
var someQuery = mgx.MustParseQuery(`{
        "id" : "$1",
        "start": { "$lte": "$2" },
        "$or": [
            { "end": { "$exists": false } },
            { "end": null },
            { "end": { "$gte": "$2" } }
        ]}`)

func QueryData(id string, date time.Time) {
    filter, err := mgx.MarshalQuery(someQuery, "$1", id, "$2", date)
    ...
}
```

## Install 

```
go get github.com/hummerd/mgx@latest
```


## Mongo Text Query Language 

The query package allows you to write mongo filters in human friendly form.

The query package is very-very experimental - use it on you own risk (or  better do not)!

``` GO
var someQuery = query.MustPrepare(`start >= "$startDate"`)

func QueryData(ctx context.Context, date time.Time) {
    filter, err := someQuery.Compile(someQuery, "$startDate",  date)
    defer filter.Discard()
    ...

    cur, err := collection.Find(ctx, filter)
}
```

### Examples:
``` GO
// simple example.
// comparison operators: =, !=, <, >, <=, >=.
// logical operators: `and`, `or`.  
var someQuery = query.MustCompile(`
       name = "some" AND
       age >= 30
    `)

func QueryStaticFilter(ctx context.Context) {
    cur, err := collection.Find(ctx, someQuery)
}
```
``` GO
// same field may be used multiple times (unlike in JSON).
var someQuery = query.MustCompile(`
       age >= 30 AND
       age <= 40
    `)

func QueryStaticFilter(ctx context.Context) {
    cur, err := collection.Find(ctx, someQuery)
}
```
``` GO
// brackets with `and` and `or` operators.
var someQuery = query.MustCompile(`
       (name = "Dima" OR name = "John") AND
       age > 25
    `)

func QueryStaticFilter(ctx context.Context) {
    cur, err := collection.Find(ctx, someQuery)
}
```
``` GO
// regexp and date type.
var someQuery = query.MustCompile(`
       name $regex /a.*/i AND
       birth = ISODate('2022-01-01T00:00:00Z')
    `)

func QueryStaticFilter(ctx context.Context) {
    cur, err := collection.Find(ctx, someQuery)
}
```
``` GO
// $in operator.
var someQuery = query.MustCompile(`
       age $in [18,27,33]
    `)

func QueryStaticFilter(ctx context.Context) {
    cur, err := collection.Find(ctx, someQuery)
}
```
``` GO
// nested fields and $exists clause.
var someQuery = query.MustCompile(`
       address.street $exists true
    `)

func QueryStaticFilter(ctx context.Context) {
    cur, err := collection.Find(ctx, someQuery)
}
```
