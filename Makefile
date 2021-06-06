GO=go
GOMOD=$(GO) mod
GOTEST=$(GO) test ./... -cover -race
GOCOVER=$(GO) tool cover
COVEROUT=./cover/c.out

.PHONY: test up down

up:
	@echo Starting the database
	docker-compose up -d

down:
	docker-compose down
	docker-compose rm --force --stop -v

build:
	cd ./cmd && $(GO) build -o ./tern-cli

test:
	@echo Starting tests
	$(GOTEST)

test/cover:
	@echo Starting tests with coverage
	$(GOTEST) -cover -coverpkg=./... -coverprofile=$(COVEROUT) . && $(GOCOVER) -html=$(COVEROUT)

deps:
	@echo Updating dependencies
	$(GOMOD) tidy
	$(GOMOD) vendor

lint:
	golangci-lint run ./...

mock: deps
	mockgen -source ./internal/database/sqlgateway/contracts.go -package sqlgateway -destination ./internal/database/sqlgateway/contracts_mock_test.go
	mockgen -source ./internal/database/sqlgateway/connector.go -package sqlgateway -destination ./internal/database/sqlgateway/connector_mock_test.go

migrate/mysql/datetime:
	./cmd/tern-cli -migrate -cfg "./stubs/cfg/tern-mysql-datetime.yaml"

migrate/mysql/timestamp:
	./cmd/tern-cli -migrate -cfg "./stubs/cfg/tern-mysql-timestamp.yaml"

rollback/mysql/datetime:
	./cmd/tern-cli -rollback -cfg "./stubs/cfg/tern-mysql-datetime.yaml"

rollback/mysql/timestamp:
	./cmd/tern-cli -rollback -cfg "./stubs/cfg/tern-mysql-timestamp.yaml"

refresh/mysql/refresh:
	./cmd/tern-cli -refresh -cfg "./stubs/cfg/tern-mysql-datetime.yaml"
