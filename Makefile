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

migrate:
	cd ./cmd && ./tern-cli -migrate -folder "./stubs/migrations/mysql" -db "mysql://tern:secret@tcp(127.0.0.1:33066)/tern_db?parseTime=true"

rollback:
	cd ./cmd && ./tern-cli -rollback -folder "./stubs/migrations/mysql" -db "mysql://tern:secret@tcp(127.0.0.1:33066)/tern_db?parseTime=true"

refresh:
	cd ./cmd && ./tern-cli -refresh -folder "./stubs/migrations/mysql" -db "mysql://tern:secret@tcp(127.0.0.1:33066)/tern_db?parseTime=true"
