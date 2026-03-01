.PHONY: install dev test lint clean docker run

install:
	pip install -e .

dev:
	pip install -e ".[dev]"

test:
	pytest tests/ -v --tb=short

test-cov:
	pytest tests/ --cov=bot --cov-report=term-missing --cov-report=html

lint:
	ruff check .

lint-fix:
	ruff check . --fix

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null; true
	rm -rf htmlcov .coverage dist build *.egg-info

docker:
	docker compose build

run:
	docker compose up -d

stop:
	docker compose down

logs:
	docker compose logs -f bot
