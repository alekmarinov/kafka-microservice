.PHONY: build
include demo_service/.env
export

build:
	docker build -t kafka-microservice .

test:
	pipenv run uwsgi --ini uwsgi.ini --mule=demo_service/demo_service.py --mule=demo_service/demo_service.py
