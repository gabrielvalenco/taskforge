.DEFAULT_GOAL := help
PYTHON ?= python3
TF_ENVS := aws azure

.PHONY: help install lint format typecheck test cov check demo clean \
        up down logs docker-build tf-fmt tf-validate

help: ## Mostra esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Instala o projeto com todas as dependências de desenvolvimento
	$(PYTHON) -m pip install -e ".[dev]"

lint: ## Ruff (lint + verificação de formato)
	ruff check .
	ruff format --check .

format: ## Corrige imports e formata o código
	ruff check . --fix
	ruff format .

typecheck: ## mypy em modo estrito
	$(PYTHON) -m mypy

test: ## Testes
	$(PYTHON) -m pytest -q

cov: ## Testes com cobertura (falha abaixo de 90%)
	$(PYTHON) -m pytest -q --cov --cov-fail-under=90

check: lint typecheck cov ## Tudo que o CI roda no Python

demo: ## Demonstração ponta a ponta em um único processo
	$(PYTHON) -m taskforge demo

up: ## Sobe Redis, worker e workers Celery (docker compose)
	docker compose up --build -d

down: ## Derruba o ambiente e remove os volumes
	docker compose down -v

logs: ## Acompanha os logs
	docker compose logs -f --tail=100

docker-build: ## Constrói a imagem
	docker build -t taskforge:dev .

tf-fmt: ## Formata os arquivos Terraform
	terraform -chdir=infra/terraform fmt -recursive

tf-validate: ## Valida os ambientes Terraform (sem backend remoto)
	@for env in $(TF_ENVS); do \
		echo "== $$env =="; \
		terraform -chdir=infra/terraform/envs/$$env init -backend=false -input=false >/dev/null && \
		terraform -chdir=infra/terraform/envs/$$env validate || exit 1; \
	done

clean: ## Remove caches e artefatos
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov build dist *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
