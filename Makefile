test:
	go test -count=1 -race ./...

cover:
	go test -coverprofile cover.out ./...

lint:
	golangci-lint run