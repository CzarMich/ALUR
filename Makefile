# Makefile for ALUR Digital Health Platform

.PHONY: run stop logs rebuild clean help

# Start services using run.sh
run:
	./run.sh

# Stop all services
stop:
	./stop.sh

# Show logs (Ctrl+C to stop)
logs:
	docker logs -f alur-app

# Rebuild image and restart
rebuild:
	docker-compose down
	docker-compose build --no-cache
	docker-compose up -d

# Remove containers, volumes, and network
clean:
	docker-compose down -v
	docker system prune -f

# Help
help:
	@echo "Usage:"
	@echo "  make run       - Start ALUR services"
	@echo "  make stop      - Stop ALUR services"
	@echo "  make logs      - Tail logs from ALUR app"
	@echo "  make rebuild   - Rebuild and restart containers"
	@echo "  make clean     - Remove containers, volumes, and unused resources"
	@echo "  make help      - Show this help message"
