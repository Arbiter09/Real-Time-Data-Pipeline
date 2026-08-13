# Real-Time Data Pipeline
#
# `make up` then `make health` is the one-command path from nothing to a
# verified stack. Everything else assumes `make health` passes.

SHELL := /bin/bash
COMPOSE := docker compose
SMOKE := -f docker-compose.yml -f docker-compose.smoke.yml
RUN_ID ?= adhoc
RATE ?= 1000
DURATION ?= 120

.DEFAULT_GOAL := help

# ---------------------------------------------------------------- lifecycle
.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the Spark, producer and Airflow images
	$(COMPOSE) build

.PHONY: up
up: ## Bring up the core stack (Kafka x3, Cassandra x3, Postgres, Spark) and create topics
	$(COMPOSE) up -d kafka1 kafka2 kafka3
	$(COMPOSE) up kafka-init
	$(COMPOSE) up -d cassandra1 cassandra2 cassandra3 postgres
	$(COMPOSE) up -d spark-master spark-worker-1 spark-worker-2 spark-driver producer
	@$(MAKE) --no-print-directory schema
	@echo
	@echo "Stack up. Verify it with: make health"

.PHONY: up-smoke
up-smoke: ## Bring up a reduced-footprint stack (correctness only, NEVER benchmark on this)
	$(COMPOSE) $(SMOKE) up -d kafka1 kafka2 kafka3
	$(COMPOSE) up kafka-init
	$(COMPOSE) $(SMOKE) up -d cassandra1 cassandra2 cassandra3 postgres
	$(COMPOSE) $(SMOKE) up -d spark-master spark-worker-1 spark-driver producer
	@$(MAKE) --no-print-directory schema

.PHONY: obs
obs: ## Add Prometheus + Grafana + kafka-exporter (Grafana on :13000)
	$(COMPOSE) --profile obs up -d
	@echo "Grafana  http://localhost:13000  (anonymous viewer enabled)"
	@echo "Prometheus http://localhost:19090"

.PHONY: airflow
airflow: ## Add Airflow scheduler + webserver (:18088, admin/admin)
	$(COMPOSE) --profile orchestration up -d
	@echo "Airflow  http://localhost:18088  (admin/admin)"

.PHONY: down
down: ## Stop everything, keep volumes
	$(COMPOSE) --profile obs --profile orchestration down

.PHONY: clean
clean: ## Stop everything and DELETE all volumes (destroys all data)
	$(COMPOSE) --profile obs --profile orchestration down -v

.PHONY: schema
schema: ## Apply the Cassandra and Postgres schemas
	@docker cp sql/cassandra/schema.cql rtdp-cassandra1:/tmp/schema.cql
	@docker exec rtdp-cassandra1 cqlsh -f /tmp/schema.cql >/dev/null && echo "  cassandra schema applied"
	@docker exec -i rtdp-postgres psql -q -U rtdp -d rtdp < sql/analytics/03_rollup_stage.sql >/dev/null && echo "  postgres rollup schema applied"

.PHONY: health
health: ## Stack gate - verifies every component is genuinely ready
	@bash scripts/healthcheck.sh

.PHONY: ps
ps: ## Show container status and memory
	@$(COMPOSE) ps --format "table {{.Name}}\t{{.Status}}"
	@echo
	@docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}"

.PHONY: logs
logs: ## Tail the streaming job's log (make logs RUN_ID=xxx)
	@tail -f results/raw/$(RUN_ID)_stream.log

# ------------------------------------------------------------------- tests
.PHONY: test
test: ## Run the full test suite
	docker exec rtdp-producer python -m pytest tests/ -v

.PHONY: test-idempotency
test-idempotency: ## Section 5: ingest the same event twice, assert counts unchanged
	docker exec rtdp-producer python -m pytest tests/test_idempotency.py -v

# -------------------------------------------------------------- measurement
.PHONY: run
run: ## One measurement run (make run RUN_ID=x RATE=1000 DURATION=120)
	python3 bench/run_scenario.py --run-id $(RUN_ID) --rate $(RATE) --duration $(DURATION)

.PHONY: skew
skew: ## Section 4: measure partition balance under each candidate key
	docker exec rtdp-producer python -m bench.partition_skew \
	  --events 200000 --report /opt/app/results/raw/partition_skew.json

.PHONY: throughput
throughput: ## Section 7.1: sweep the producer rate to the lag-flat ceiling
	python3 bench/throughput_sweep.py --duration $(DURATION)

.PHONY: latency
latency: ## Section 7.2: p50/p95/p99 from sampled writes (make latency RUN_ID=x)
	docker exec rtdp-producer python -m bench.latency_report \
	  --run-id $(RUN_ID) --report /opt/app/results/raw/$(RUN_ID)_latency.json

.PHONY: chaos
chaos: ## Section 7.3: kill scenarios + the backoff A/B, repeated
	python3 bench/chaos_suite.py --repeats 3

.PHONY: analytics-load
analytics-load: ## Section 8: load the corpus into both schemas
	docker exec rtdp-producer python -m bench.load_analytics \
	  --rows 3000000 --days 30

.PHONY: analytics
analytics: ## Section 8: baseline vs star schema, per-query ratios
	docker exec rtdp-producer python -m bench.analytics_bench \
	  --iterations 9 --warmups 3

# --------------------------------------------------------------- reliability
.PHONY: dlq
dlq: ## Inspect the dead letter queue
	docker exec rtdp-producer python -m replay.dlq_tools inspect

.PHONY: dlq-drain
dlq-drain: ## Drain the DLQ back into the pipeline
	docker exec rtdp-producer python -m replay.dlq_tools drain

.PHONY: dlq-dry-run
dlq-dry-run: ## Show what a drain would do, without doing it
	docker exec rtdp-producer python -m replay.dlq_tools drain --dry-run

# --------------------------------------------------------------------- misc
.PHONY: stream
stream: ## Start the ingest stream in the foreground (Ctrl-C to stop)
	docker exec -e RUN_ID=$(RUN_ID) rtdp-spark-driver \
	  bash /opt/app/scripts/submit-stream.sh --run-id $(RUN_ID) --duration 0

.PHONY: produce
produce: ## Produce events (make produce RATE=1000 DURATION=60)
	docker exec rtdp-producer python -m producer.produce \
	  --rate $(RATE) --duration $(DURATION) --run-id $(RUN_ID)

.PHONY: restore-others
restore-others: ## Restart the other Docker stacks this project displaced
	@bash scripts/restore-other-stacks.sh
