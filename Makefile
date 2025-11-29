DEV_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.dev.yml

.PHONY: dev dev-detach stop logs

dev:
	$(DEV_COMPOSE) up --build

dev-detach:
	$(DEV_COMPOSE) up --build -d

stop:
	docker compose down

logs:
	docker compose logs -f
