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