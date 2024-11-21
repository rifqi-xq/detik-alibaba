ANALYTIC_COLLECTOR_PKG_VERSION=1.2.3
COMMIT=`git rev-parse --short HEAD`

build: build_binary

build-mac:
	@GOOS=darwin GOARCH=amd64 \
	go build -v --ldflags

install-linux:
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go install -v --ldflags

build_binary:
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o analytic-collector -a --ldflags 

test:
	@go test -v $(shell go list ./... | grep -v /vendor/)

vet:
	@go vet -v $(shell go list ./... | grep -v /vendor/)

clean:
	@rm -rf build
	@rm -rf analytic-collector*

.PHONY: test vet build build_binary clean
