build:
	@CGO_ENABLED=0 GOARCH=$(go env GOARCH) GOOS=$(go env GOOS) go build -o ./bin/wal -a -ldflags="-s -w" -installsuffix cgo cmd/wal/main.go

run: build
	@./bin/wal