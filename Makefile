ifneq (,$(wildcard .env))
include .env
endif

.PHONY: test deps gen gen-sdk gen-web gen-all run agent-run

test:
	@echo "Running tests"
	go clean -testcache
	go test ./... -v

deps:
	@echo "Installing local tooling"
	go install github.com/swetjen/virtuous@latest
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
	go install github.com/cespare/reflex@latest

gen:
	@echo "Generating sqlc output"
	cd db/sql && sqlc generate

gen-sdk:
	@echo "Generating SDKs"
	go run ./cmd/clients

gen-web:
	@echo "Building frontend assets"
	cd frontend-web && bun install && bun run build

gen-all:
	@echo "Generating sqlc output, SDKs, and frontend assets"
	make gen
	make gen-sdk
	make gen-web

run:
	reflex -r '(^|/)(cmd|config|dag|db|deps|handlers|frontend-web/src)/.*\.(go|html|js|ts|tsx)$$|(^|/)router\.go$$|(^|/)frontend_embed\.go$$|(^|/)client_gen\.go$$|(^|/)\.env$$' -s -- sh -c "go run ./cmd/api"

agent-run:
	@bash -c ': > ERRORS; reflex -r '\''(^|/)(cmd|config|dag|db|deps|handlers|frontend-web/src)/.*\.(go|html|js|ts|tsx)$$|(^|/)router\.go$$|(^|/)frontend_embed\.go$$|(^|/)client_gen\.go$$|(^|/)\.env$$'\'' -s -- sh -c "go run ./cmd/api" 2>&1 | tee ERRORS'
