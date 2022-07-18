test:
	go test -count=1 -race ./...

lint:
	golangci-lint run