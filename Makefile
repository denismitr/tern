GO=go
GOTEST=$(GO) test ./... -race
GOCOVER=$(GO) tool cover
COVEROUT=./cover/c.out

.PHONY: test up down

up:
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

migrate:
	cd ./cmd && ./tern-cli -migrate -folder "/home/denismitr/code/golang/tern/stubs/migrations/mysql" -db "mysql://tern:secret@tcp(127.0.0.1:33066)/tern_db?parseTime=true"

rollback:
	cd ./cmd && ./tern-cli -rollback -folder "/home/denismitr/code/golang/tern/stubs/migrations/mysql" -db "mysql://tern:secret@tcp(127.0.0.1:33066)/tern_db?parseTime=true"

refresh:
	cd ./cmd && ./tern-cli -refresh -folder "/home/denismitr/code/golang/tern/stubs/migrations/mysql" -db "mysql://tern:secret@tcp(127.0.0.1:33066)/tern_db?parseTime=true"
