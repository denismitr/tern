up:
	docker-compose up -d

down:
	docker-compose down
	docker-compose rm --force --stop -v