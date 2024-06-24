#!/bin/bash
set -euxo pipefail

poetry run isort ml_scheduler/ tests/
poetry run black ml_scheduler/ tests/
