requirements:
	pip freeze > requirements.txt

build:
	docker compose build

up:
	rm -rf dist/*
	docker compose up

lambda:
	pip install -t dist/lambda .
	cd dist/lambda && zip -x '*.pyc' -r ../lambda.zip .
