.PHONY: tidy proto up down test e2e fmt

tidy:        ## resolve dependencies and write go.sum
	go mod tidy

proto:       ## regenerate gen/ (needs protoc, protoc-gen-go, protoc-gen-go-grpc)
	protoc -I proto \
	  --go_out=gen --go_opt=module=github.com/example/orderflow/gen \
	  --go-grpc_out=gen --go-grpc_opt=module=github.com/example/orderflow/gen \
	  proto/orders/v1/orders.proto

up:          ## start the full stack
	docker compose -f deploy/docker-compose.yml up --build

down:
	docker compose -f deploy/docker-compose.yml down -v

test:        ## unit tests
	go test ./...

e2e:         ## end-to-end tests against a running stack (make up first)
	go test -tags e2e -v -count=1 ./test/e2e/

fmt:
	gofmt -w .
