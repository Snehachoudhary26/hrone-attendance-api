# Employee Attendance & Analytics API

A FastAPI and MongoDB backend service for employee attendance tracking, regularization audit trails, and high-performance analytics pipelines (Contract v2.0).

---

## Requirements
- Python 3.11+
- MongoDB 6.0+ (Tested on MongoDB 7.0)

---

## Local Setup

1. Create and activate a virtual environment:
   python3 -m venv .venv
   source .venv/bin/activate

2. Install dependencies:
   pip install -r requirements.txt

3. Configure Environment:
   export MONGO_URI="mongodb://localhost:27017"
   export MONGO_DB="attendance_db"

4. Run the API server:
   uvicorn app.main:app --port 8000 --reload

5. Interactive Documentation:
   - Swagger UI: http://localhost:8000/docs
   - ReDoc: http://localhost:8000/redoc

---

## Project Structure
- app/main.py: Complete FastAPI application with all endpoints, models, business rules (R1-R10), and aggregation pipelines.
- requirements.txt: Python package dependencies.
- REVIEW.md: Comprehensive review of the starter code with 19 identified defects and fixes.
- DECISIONS.md: Architectural decisions, indexing strategy, race condition handling, and scalability analysis.
- README.md: Setup and run instructions.

---

## Key Features & Architecture
- Idempotent Indexing: All primary compound and unique indexes are automatically created at startup.
- Race Condition Prevention: Atomic database operations (find_one_and_update) and unique compound indexes on (emp_code, date) prevent duplicate punch-ins and race conditions under concurrent requests.
- Pure MongoDB Aggregations: Analytics endpoints utilize aggregation pipelines (, , , ) rather than in-memory loops.
- Explain Plans: GET /admin/explain/{endpoint} exposes execution plans confirming index scans (IXSCAN) and zero collection scans (COLLSCAN).
