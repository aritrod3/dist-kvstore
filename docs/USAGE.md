# Usage

## Local Tests
- `make test` or `go test ./...`
- `go test ./internal/storage -bench=.`

## Local Cluster
- `docker compose -f deploy/docker-compose.yml up`

## Metrics
- Each node exposes a Prometheus scrape endpoint (see `metrics_address` in `deploy/node-*.yaml`).
