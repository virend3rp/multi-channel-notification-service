.PHONY: up down logs test e2e e2e-compose vet build ui load

up:            ## start the full stack
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f api worker

build:
	go build ./...

vet:
	go vet ./... && go vet -tags e2e ./...

test:          ## unit tests (no Docker needed)
	go test ./...

e2e:           ## Cucumber features on an in-process stack backed by Testcontainers
	go test -tags e2e -count=1 -timeout 20m ./test/e2e/

e2e-compose:   ## Cucumber features against `make up` (relax the breaker first: see README)
	E2E_API_URL=http://localhost:8080 E2E_STUBS_URL=http://localhost:8090 \
		go test -tags e2e -count=1 -timeout 20m ./test/e2e/

ui:
	cd admin-ui && npm install && npm start

load:          ## k6 load test against the running stack
	k6 run load-tests/send.js
