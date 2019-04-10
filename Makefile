GOFILES=$(shell find . -type f -name '*.go')

migrate-management-beats: $(GOFILES)
	go build -ldflags "-X main.build=$(shell date -u '+%Y-%m-%dT%H:%M:%SZ') -X main.commit=$(shell git rev-parse HEAD)"
