UV ?= uv
UV_CACHE_DIR ?= $(CURDIR)/.local/uv-cache
export UV_CACHE_DIR
UV_RUN = $(UV) run --locked --all-extras
SMOKE_FILE = data/mbp-10/2022/01/03/glbx-mdp3-20220103.mbp-10.parquet
FOOTER_MANIFEST = data/derived/manifests/mbp10_footer_manifest_v1.jsonl
HASH_MANIFEST = data/derived/manifests/mbp10_source_sha256_v1.jsonl
QC_CONFIG = configs/data/mbp10_structural_qc_v1.toml
QC_MANIFEST = data/derived/manifests/mbp10_structural_qc_v1.jsonl
QC_MUTATION_MANIFEST = data/derived/manifests/mbp10_clean_trade_none_book_mutations_v1.jsonl
QC_PROGRESS_EVERY ?= 250

.PHONY: setup db-init db-start db-stop db-status db-bootstrap db-bootstrap-test db-up catalog hash data-register qc qc-inspect qc-register smoke pilot doctor test lint format notebook research-ready m0b-real-slice m0b-active-contract m0b-schedule-fixture m0b-status-fixture m0b-first-passage m0b-worker-cycle m0b-pg-gate m0b-holdout-verify m0b-worker-verify

setup:
	$(UV) sync --all-extras --locked

db-init:
	$(UV_RUN) systematic-fx db local init

db-start:
	$(UV_RUN) systematic-fx db local start

db-stop:
	$(UV_RUN) systematic-fx db local stop

db-status:
	$(UV_RUN) systematic-fx db local status

db-bootstrap:
	$(UV_RUN) systematic-fx db bootstrap

db-bootstrap-test: db-start
	$(UV_RUN) systematic-fx db bootstrap-test

db-up: db-start db-bootstrap

catalog:
	$(UV_RUN) systematic-fx data catalog --manifest $(FOOTER_MANIFEST)

hash: catalog
	$(UV_RUN) systematic-fx data hash --footer-manifest $(FOOTER_MANIFEST)

data-register: hash db-up
	$(UV_RUN) systematic-fx data register --footer-manifest $(FOOTER_MANIFEST) --hash-manifest $(HASH_MANIFEST)

qc: hash
	$(UV_RUN) systematic-fx data qc --config $(QC_CONFIG) --source-manifest $(HASH_MANIFEST) --manifest-name $(notdir $(QC_MANIFEST)) --progress-every $(QC_PROGRESS_EVERY)

qc-inspect:
	$(UV_RUN) systematic-fx data inspect-qc-mutations --qc-manifest $(QC_MANIFEST) --output-name $(notdir $(QC_MUTATION_MANIFEST))

qc-register: db-up
	$(UV_RUN) systematic-fx data register-qc --scan-manifest $(QC_MANIFEST) --source-manifest $(HASH_MANIFEST)

smoke:
	$(UV_RUN) systematic-fx data smoke $(SMOKE_FILE) --row-groups 1

pilot:
	$(UV_RUN) systematic-fx features pilot $(SMOKE_FILE) --instrument-id 28727 --symbol 6EH2 --source-date 2022-01-03

doctor:
	$(UV_RUN) systematic-fx doctor --require-database

test: db-bootstrap-test
	$(UV_RUN) pytest

lint:
	$(UV_RUN) ruff check .
	$(UV_RUN) ruff format --check .

format:
	$(UV_RUN) ruff check --fix .
	$(UV_RUN) ruff format .

notebook:
	$(UV_RUN) jupyter lab

research-ready: setup data-register smoke test doctor

m0b-real-slice:
	$(UV_RUN) systematic-fx research m0b materialize-real-slice

m0b-active-contract:
	$(UV_RUN) systematic-fx research m0b verify-active-contract-mapping \
		--allow-bounded-weekday-fallback

m0b-schedule-fixture:
	$(UV_RUN) systematic-fx research m0b verify-schedule-archive \
		--archive tests/fixtures/cme_schedule_archive_fixture_v1.toml \
		--source tests/fixtures/cme_schedule_upstream_fixture_v1.txt \
		--allow-test-fixture

m0b-status-fixture:
	$(UV_RUN) systematic-fx research m0b verify-status-evidence \
		--evidence tests/fixtures/cme_trading_status_fixture_v1.toml \
		--source tests/fixtures/cme_trading_status_upstream_fixture_v1.txt \
		--allow-test-fixture

m0b-first-passage:
	$(UV_RUN) systematic-fx research m0b build-first-passage-store \
		--build artifacts/research/m0b_real_slice_v1/build-17f4ccdcb839c70bfdd95c9d00a2b37ca6d31fff89c34439a2adcaac4c32cf5f.json \
		--store-root artifacts/research/m0b_first_passage_store_v1

m0b-worker-cycle:
	$(UV_RUN) systematic-fx research m0b worker-cycle \
		--epoch-key "$(M0B_EPOCH_KEY)" \
		--worker-id "$${M0B_WORKER_ID:-m0b-worker-1}" \
		--worker-root artifacts/research/m0b_worker

m0b-pg-gate:
	SYSTEMATIC_FX_RUN_M0B_PG_GATE=1 $(UV_RUN) pytest \
		tests/integration/test_m0b_control_plane_postgres.py \
		tests/integration/test_m0b_holdout_provisioning_postgres.py \
		tests/integration/test_m0b_worker_capability_postgres.py -q -s

m0b-holdout-verify:
	$(UV_RUN) systematic-fx db verify-holdout-isolation --json

m0b-worker-verify:
	$(UV_RUN) systematic-fx db verify-m0b-worker-access --json
