# ARGS:
# V: version - ex: v1.0.0

.DEFAULT_GOAL=build_bin

build_bin:
	go build -o bin/server ./cmd/server 

build_docker:
	docker build -t gcr.io/rowan-senior-project/tensorbeat-datalake:$(V) .

push_docker: build_docker
	docker push gcr.io/rowan-senior-project/tensorbeat-datalake:$(V)
