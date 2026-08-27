SHELL := /bin/bash
.DEFAULT_GOAL := help

GO ?= go
GO_ENV ?= GOTOOLCHAIN=go1.24.3 GOWORK=off
COMPOSE ?= $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

.PHONY: help status image compose-check
help:
	@echo "TurboCache — Mac 本地开发入口"
	@echo "  build / test / vet / fmt-check / all  构建与验证（test 含 race）"
	@echo "  up / down / status / logs            本地 Compose 集群（down 保留卷）"
	@echo "  image / compose-check               构建镜像 / 校验配置"
	@echo "  integration                         启动本地集群并验证基本链路"
	@echo "  resume-metrics / resume-failover      基线 / 节点故障实验"
	@echo "  benchmark-matrix / bloom-experiment  阶梯 / Bloom 对照实验"
	@echo "实验会操作项目容器；详情见 scripts/README.md。"

status:
	$(COMPOSE) -f deployments/docker-compose.yml ps

image:
	$(COMPOSE) -f deployments/docker-compose.yml build

compose-check:
	$(COMPOSE) -f deployments/docker-compose.yml config --quiet

.PHONY: all
all: fmt-check vet test build

.PHONY: build
build:
	mkdir -p bin
	$(GO_ENV) $(GO) build -o bin/turbocached ./cmd/turbocached
	$(GO_ENV) $(GO) build -o bin/turbocachectl ./cmd/turbocachectl
	$(GO_ENV) $(GO) build -o bin/turbocache-benchmark ./cmd/benchmark

.PHONY: test
test:
	$(GO_ENV) $(GO) test ./... -race

.PHONY: fmt-check
fmt-check:
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*' -not -path './api/generated/*'))" || \
		(echo "gofmt is required"; gofmt -l $$(find . -name '*.go' -not -path './vendor/*' -not -path './api/generated/*'); exit 1)

.PHONY: fmt
fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*' -not -path './api/generated/*')

.PHONY: vet
vet:
	$(GO_ENV) $(GO) vet ./...

.PHONY: up
up:
	$(COMPOSE) -f deployments/docker-compose.yml up -d --build

.PHONY: down
down:
	$(COMPOSE) -f deployments/docker-compose.yml down

.PHONY: logs
logs:
	$(COMPOSE) -f deployments/docker-compose.yml logs -f --tail=200

.PHONY: integration
integration:
	bash scripts/integration/basic.sh

.PHONY: resume-metrics
resume-metrics:
	bash scripts/experiments/resume_metrics.sh baseline

.PHONY: resume-failover
resume-failover:
	bash scripts/experiments/resume_metrics.sh failover

.PHONY: benchmark-matrix
benchmark-matrix:
	bash scripts/experiments/benchmark_matrix.sh

.PHONY: bloom-experiment
bloom-experiment:
	bash scripts/experiments/bloom_experiment.sh
