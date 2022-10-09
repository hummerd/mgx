test:
	go test -v -count=1 -race ./...

test.integration:
	MGX_INTEGRATION_MONGO_URI="mongodb://admin:password@localhost:27017" \
	go test -v -count=1 -race ./query/realdb_test.go

cover:
	go test -coverprofile cover.out ./...

lint:
	golangci-lint run