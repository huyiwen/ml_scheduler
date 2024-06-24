#!/bin/bash
set -euxo pipefail

poetry run cruft check
poetry run mypy --ignore-missing-imports ml_scheduler/
poetry run isort --check --diff ml_scheduler/ tests/
poetry run black --check ml_scheduler/ tests/
poetry run flake8 ml_scheduler/ tests/
poetry run safety check -i 39462 -i 40291
poetry run bandit -r ml_scheduler/
