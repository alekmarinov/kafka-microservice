.PHONY: build run

build:
	docker build -t demo-service .

run:
	docker run --rm -it -p 8080:8080 --env-file .env demo-service
