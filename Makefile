TAG 		:= "1.0.0"
VERSION := "1.0.0-$(shell git rev-parse --short HEAD)"

build:
	@CGO_ENABLED=0 GOARCH=$(go env GOARCH) GOOS=$(go env GOOS) go build -o ./bin/wal -a -ldflags="-s -w" -installsuffix cgo cmd/wal/main.go

run: build
	@./bin/wal

gen-pb:
	@protoc \
  --go_out=internal/core/domain/proto \
  --go_opt=module=github.com/iamNilotpal/wal \
  --proto_path=pkg/protobuf \
  pkg/protobuf/entry.proto

tag:
	git tag $(TAG)

tag-push:
	git push --tags