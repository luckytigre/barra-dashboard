.PHONY: dev backend frontend refresh setup universe rebuild-source

setup:
	cd backend && pip install -e ".[dev]"
	cd frontend && npm install

backend:
	cd backend && uvicorn main:app --reload --port 8000

frontend:
	cd frontend && npm run dev -- --port 3000

dev:
	@echo "Starting backend and frontend..."
	$(MAKE) backend & $(MAKE) frontend & wait

refresh:
	curl -X POST http://localhost:8000/api/refresh

universe:
	python3 backend/scripts/build_universe_eligibility_lseg.py --db-path backend/data.db

rebuild-source:
	python3 backend/scripts/reset_and_rebuild_source_of_truth.py --db-path backend/data.db
