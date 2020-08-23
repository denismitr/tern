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

test/cover:
	$(GOTEST) -cover -coverprofile=$(COVEROUT) . && $(GOCOVER) -html=$(COVEROUT)