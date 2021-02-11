# ARGS:
# V: version - ex: v1.0.0

.DEFAULT_GOAL=build

build:
	go build -o bin/server ./cmd/server 

docker_build:
	docker build -t gcr.io/rowan-senior-project/tensorbeat-datalake:$(V) .

docker_push: docker_build
	docker push gcr.io/rowan-senior-project/tensorbeat-datalake:$(V)
