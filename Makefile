.PHONY: install test lint dashboard docs validate deploy bootstrap daily

TARGET ?= dev

install:        ## local dev environment (needs Java 17+ for the Spark tests)
	pip install -e ".[dev]"

test:           ## unit + local end-to-end pipeline tests
	pytest

lint:
	ruff check src tests tools

dashboard:      ## regenerate the AI/BI dashboard JSON from tools/build_dashboard.py
	python tools/build_dashboard.py

docs:           ## regenerate docs/metric_definitions.md from the metric catalog
	python tools/gen_metric_docs.py

validate:
	databricks bundle validate -t $(TARGET)

deploy: validate
	databricks bundle deploy -t $(TARGET)

bootstrap:      ## one-off: land history, build all layers, publish semantic layer, governance
	databricks bundle run wm_bootstrap -t $(TARGET)

daily:
	databricks bundle run wm_daily_reporting -t $(TARGET)
