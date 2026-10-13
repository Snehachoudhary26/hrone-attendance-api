"""
Employee Attendance & Analytics API (FastAPI + MongoDB)
Implements contract v2.0 with business rules R1-R10, atomic concurrency controls,
and aggregation pipelines for analytics and explain plans.
"""
import calendar
import json
import math
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Path, status
from pydantic import BaseModel, Field, field_validator, model_validator
import pymongo
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

client = MongoClient(MONGO_URI, tz_aware=True)
db = client[MONGO_DB]

IST = timezone(timedelta(hours=5, minutes=30))


def create_indexes():
    """Create all required indexes idempotently at startup."""
    # Employees collection indexes
    db.employees.create_index([("emp_code", 1)], unique=True)
    db.employees.create_index([("department", 1), ("joined_on", 1)])
    db.employees.create_index([("joined_on", 1)])

    # Attendance logs collection indexes
    db.attendance_logs.create_index([("emp_code", 1), ("date", 1)], unique=True)
    db.attendance_logs.create_index([("emp_code", 1), ("date", -1)])
    db.attendance_logs.create_index([("date", -1), ("emp_code", 1)])
    db.attendance_logs.create_index([("date", 1), ("emp_code", 1)])
    db.attendance_logs.create_index([("emp_code", 1), ("punch_in", -1)])
    db.attendance_logs.create_index([("date", 1), ("late_minutes", 1), ("emp_code", 1)])
    db.attendance_logs.create_index([("date", 1), ("status", 1)])


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_indexes()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# Helper Functions (Rules R1 - R8)
# --------------------------------------------------------------------------- #
def truncate_ms(ms: int) -> int:
    """Truncates instant to whole seconds before calculation per Rule R1."""
    return (ms // 1000) * 1000


def ms_to_dt(ms: Optional[int]) -> Optional[datetime]:
    """Converts epoch milliseconds to UTC datetime for BSON storage."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    """Converts BSON datetime to epoch milliseconds for API response."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def now_ms() -> int:
    """Returns current UTC time in epoch milliseconds truncated to whole seconds."""
    return truncate_ms(int(datetime.now(timezone.utc).timestamp() * 1000))


def round_half_up_2(val: Optional[float]) -> Optional[float]:
    """Rounds floating point numbers to 2 decimals half-up per Rule R8."""
    if val is None:
        return None
    return float(Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def round_half_up_4(val: Optional[float]) -> Optional[float]:
    """Rounds floating point rates to 4 decimals half-up per Rule R8."""
    if val is None:
        return None
    return float(Decimal(str(val)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def get_attendance_date(punch_in_ms: int, shift_start: str, shift_end: str) -> str:
    """Determines the IST attendance date, accounting for overnight shifts per Rule R1."""
    punch_dt_ist = datetime.fromtimestamp(punch_in_ms / 1000, tz=IST)
    h_start, m_start = map(int, shift_start.split(":"))
    h_end, m_end = map(int, shift_end.split(":"))

    is_overnight = (h_end < h_start) or (h_end == h_start and m_end <= m_start)
    if is_overnight and punch_dt_ist.time() < time(h_end, m_end):
        return (punch_dt_ist.date() - timedelta(days=1)).isoformat()
    return punch_dt_ist.date().isoformat()


def compute_late_minutes(punch_in_ms: int, shift_start: str, attendance_date: str) -> int:
    """Computes late minutes per Rule R2 (10-minute grace period)."""
    h_start, m_start = map(int, shift_start.split(":"))
    shift_start_dt = datetime.fromisoformat(attendance_date).replace(
        hour=h_start, minute=m_start, second=0, microsecond=0, tzinfo=IST
    )
    punch_dt_ist = datetime.fromtimestamp(punch_in_ms / 1000, tz=IST)
    elapsed_seconds = (punch_dt_ist - shift_start_dt).total_seconds()
    if elapsed_seconds > 600:
        return int(math.floor(elapsed_seconds / 60))
    return 0


def compute_work_hours(punch_in_ms: int, punch_out_ms: int) -> float:
    """Computes work hours rounded to 2 decimals half-up per Rule R4."""
    elapsed_seconds = (punch_out_ms - punch_in_ms) // 1000
    hours = Decimal(elapsed_seconds) / Decimal(3600)
    return float(hours.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(punch_out_ms: int, shift_start: str, shift_end: str, attendance_date: str) -> int:
    """Computes overtime in whole minutes (minimum 30 min) per Rule R3."""
    h_start, m_start = map(int, shift_start.split(":"))
    h_end, m_end = map(int, shift_end.split(":"))
    is_overnight = (h_end < h_start) or (h_end == h_start and m_end <= m_start)

    base_date = datetime.fromisoformat(attendance_date)
    if is_overnight:
        end_dt = (base_date + timedelta(days=1)).replace(
            hour=h_end, minute=m_end, second=0, microsecond=0, tzinfo=IST
        )
    else:
        end_dt = base_date.replace(
            hour=h_end, minute=m_end, second=0, microsecond=0, tzinfo=IST
        )

    punch_out_ist = datetime.fromtimestamp(punch_out_ms / 1000, tz=IST)
    elapsed_seconds = (punch_out_ist - end_dt).total_seconds()
    minutes = int(math.floor(elapsed_seconds / 60))
    if minutes >= 30:
        return minutes
    return 0


def get_working_days(month_str: str, joined_on_str: Optional[str] = None) -> int:
    """Calculates Mon-Fri working days for a month, considering mid-month joiners per Rule R7."""
    year, month = map(int, month_str.split("-"))
    num_days = calendar.monthrange(year, month)[1]
    start_day = 1
    if joined_on_str:
        joined_date = datetime.fromisoformat(joined_on_str).date()
        if joined_date.year > year or (joined_date.year == year and joined_date.month > month):
            return 0
        if joined_date.year == year and joined_date.month == month:
            start_day = max(1, joined_date.day)

    working_days = 0
    for d in range(start_day, num_days + 1):
        if date(year, month, d).weekday() < 5:  # Monday to Friday
            working_days += 1
    return working_days


def format_history_entry(entry: dict) -> dict:
    """Serializes history audit entry to API contract."""
    at_val = entry.get("at")
    if isinstance(at_val, datetime):
        at_val = dt_to_ms(at_val)

    changes_formatted = {}
    changes = entry.get("changes", {})
    for field, change in changes.items():
        from_val = change.get("from")
        to_val = change.get("to")
        if field in ("punch_in", "punch_out"):
            if isinstance(from_val, datetime):
                from_val = dt_to_ms(from_val)
            if isinstance(to_val, datetime):
                to_val = dt_to_ms(to_val)
        changes_formatted[field] = {"from": from_val, "to": to_val}

    return {
        "at": at_val,
        "by": entry.get("by"),
        "reason": entry.get("reason"),
        "changes": changes_formatted,
    }


def format_attendance_doc(doc: dict) -> dict:
    """Serializes internal attendance document to OpenAPI AttendanceRecord."""
    history = doc.get("history", [])
    formatted_history = [format_history_entry(h) for h in history]
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes", 0),
        "overtime_minutes": doc.get("overtime_minutes", 0),
        "half_day": doc.get("half_day", False),
        "history": formatted_history,
    }


def format_employee_doc(doc: dict) -> dict:
    """Serializes employee document to OpenAPI Employee."""
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": dt_to_ms(doc.get("created_at")),
    }


# --------------------------------------------------------------------------- #
# Request Models
# --------------------------------------------------------------------------- #
class EmployeeCreate(BaseModel):
    emp_code: str = Field(..., pattern=r"^EMP\d{4,6}$")
    name: str = Field(..., min_length=1, max_length=100)
    email: str = Field(..., max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(..., min_length=1, max_length=50)
    shift_start: str = Field("09:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    shift_end: str = Field("18:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def validate_joined_on(cls, v: str) -> str:
        try:
            date.fromisoformat(v)
        except ValueError:
            raise ValueError("joined_on must be YYYY-MM-DD")
        return v

    @model_validator(mode="after")
    def validate_shift_times(self) -> "EmployeeCreate":
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[int] = Field(None, ge=100000000000, le=4102444800000)
    status: str = Field("PRESENT", pattern=r"^(PRESENT|WFH|ON_DUTY)$")


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[int] = Field(None, ge=100000000000, le=4102444800000)


class RegularizeRequest(BaseModel):
    status: Optional[str] = Field(None, pattern=r"^(PRESENT|ABSENT|LEAVE|WFH|ON_DUTY)$")
    punch_in: Optional[int] = Field(None, ge=100000000000, le=4102444800000)
    punch_out: Optional[int] = Field(None, ge=100000000000, le=4102444800000)
    reason: str = Field(..., min_length=5, max_length=200)
    regularized_by: str = Field(..., min_length=1, max_length=50)


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health", tags=["system"])
def health():
    try:
        client.admin.command("ping")
        return {"status": "ok"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {str(exc)}")


@app.post("/employees", status_code=201, tags=["employees"])
def create_employee(body: EmployeeCreate):
    created_dt = datetime.now(timezone.utc)
    doc = body.model_dump()
    doc["created_at"] = created_dt

    try:
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="emp_code already exists")

    return format_employee_doc(doc)


@app.get("/employees", tags=["employees"])
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q = {}
    if department is not None:
        q["department"] = department

    total = db.employees.count_documents(q)
    skip = (page - 1) * page_size
    cursor = db.employees.find(q).sort("emp_code", 1).skip(skip).limit(page_size)
    items = [format_employee_doc(doc) for doc in cursor]

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201, tags=["attendance"])
def punch_in(body: PunchInRequest):
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    punch_ms = truncate_ms(body.punched_at if body.punched_at is not None else now_ms())
    att_date = get_attendance_date(punch_ms, emp["shift_start"], emp["shift_end"])
    late_mins = compute_late_minutes(punch_ms, emp["shift_start"], att_date)
    punch_dt = ms_to_dt(punch_ms)

    doc = {
        "emp_code": body.emp_code,
        "date": att_date,
        "status": body.status,
        "punch_in": punch_dt,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": late_mins,
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }

    try:
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="Already punched in for this date")

    return format_attendance_doc(doc)


@app.post("/attendance/punch-out", status_code=200, tags=["attendance"])
def punch_out(body: PunchOutRequest):
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    punch_out_ms = truncate_ms(body.punched_at if body.punched_at is not None else now_ms())
    punch_out_dt = ms_to_dt(punch_out_ms)

    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": punch_out_dt}},
        sort=[("punch_in", -1)],
    )
    if not rec:
        raise HTTPException(status_code=404, detail="No punch-in found")

    if rec.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="Already punched out")

    punch_in_ms = dt_to_ms(rec["punch_in"])
    if punch_out_ms <= punch_in_ms:
        raise HTTPException(status_code=422, detail="punch_out must be after punch_in")

    if (punch_out_ms - punch_in_ms) > 24 * 3600 * 1000:
        raise HTTPException(status_code=422, detail="punch_out cannot be more than 24 hours after punch_in")

    work_hours = compute_work_hours(punch_in_ms, punch_out_ms)
    half_day = bool(work_hours < 4.50)
    overtime_mins = compute_overtime(
        punch_out_ms, emp["shift_start"], emp["shift_end"], rec["date"]
    )

    # Atomic update to guarantee single punch-out under concurrency
    updated = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None},
        {
            "$set": {
                "punch_out": punch_out_dt,
                "work_hours": work_hours,
                "overtime_minutes": overtime_mins,
                "half_day": half_day,
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="Already punched out")

    return format_attendance_doc(updated)


@app.get("/attendance", tags=["attendance"])
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    date_to: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    status: Optional[str] = Query(None, pattern=r"^(PRESENT|ABSENT|LEAVE|WFH|ON_DUTY)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="date_from cannot be greater than date_to")

    q = {}
    if emp_code:
        q["emp_code"] = emp_code
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to
    if status:
        q["status"] = status

    total = db.attendance_logs.count_documents(q)
    skip = (page - 1) * page_size
    cursor = (
        db.attendance_logs.find(q)
        .sort([("date", -1), ("emp_code", 1)])
        .skip(skip)
        .limit(page_size)
    )
    items = [format_attendance_doc(d) for d in cursor]

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}", tags=["attendance"])
def regularize_attendance(
    emp_code: str = Path(...),
    date: str = Path(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    body: RegularizeRequest = ...,
):
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if not rec:
        raise HTTPException(status_code=404, detail="Attendance record not found")

    target_status = body.status if body.status is not None else rec["status"]
    curr_in_ms = dt_to_ms(rec.get("punch_in"))
    curr_out_ms = dt_to_ms(rec.get("punch_out"))

    if target_status in ("ABSENT", "LEAVE"):
        if body.punch_in is not None or body.punch_out is not None:
            raise HTTPException(
                status_code=422, detail="Cannot supply punch times when setting status to ABSENT or LEAVE"
            )
        target_in_ms = None
        target_out_ms = None
    else:
        # Presence status
        if body.punch_in is not None:
            target_in_ms = truncate_ms(body.punch_in)
        else:
            target_in_ms = curr_in_ms

        if target_in_ms is None:
            raise HTTPException(status_code=422, detail="Presence status requires a punch_in")

        if body.punch_in is not None:
            calc_date = get_attendance_date(target_in_ms, emp["shift_start"], emp["shift_end"])
            if calc_date != date:
                raise HTTPException(status_code=422, detail="punch_in does not match record attendance date")

        if body.punch_out is not None:
            target_out_ms = truncate_ms(body.punch_out)
        else:
            target_out_ms = curr_out_ms

        if target_out_ms is not None:
            if target_out_ms <= target_in_ms:
                raise HTTPException(status_code=422, detail="punch_out must be after punch_in")
            if (target_out_ms - target_in_ms) > 24 * 3600 * 1000:
                raise HTTPException(
                    status_code=422, detail="punch_out cannot be more than 24 hours after punch_in"
                )

    # Compute target derived fields
    if target_status in ("ABSENT", "LEAVE"):
        target_late = 0
        target_work_hours = None
        target_ot = 0
        target_half_day = False
    else:
        target_late = compute_late_minutes(target_in_ms, emp["shift_start"], date)
        if target_out_ms is not None:
            target_work_hours = compute_work_hours(target_in_ms, target_out_ms)
            target_half_day = bool(target_work_hours < 4.50)
            target_ot = compute_overtime(target_out_ms, emp["shift_start"], emp["shift_end"], date)
        else:
            target_work_hours = None
            target_half_day = False
            target_ot = 0

    # Determine changes
    mongo_changes = {}
    if target_status != rec.get("status"):
        mongo_changes["status"] = {"from": rec.get("status"), "to": target_status}

    if target_in_ms != curr_in_ms:
        mongo_changes["punch_in"] = {
            "from": ms_to_dt(curr_in_ms),
            "to": ms_to_dt(target_in_ms),
        }

    if target_out_ms != curr_out_ms:
        mongo_changes["punch_out"] = {
            "from": ms_to_dt(curr_out_ms),
            "to": ms_to_dt(target_out_ms),
        }

    if target_work_hours != rec.get("work_hours"):
        mongo_changes["work_hours"] = {"from": rec.get("work_hours"), "to": target_work_hours}

    if target_late != rec.get("late_minutes", 0):
        mongo_changes["late_minutes"] = {"from": rec.get("late_minutes", 0), "to": target_late}

    if target_ot != rec.get("overtime_minutes", 0):
        mongo_changes["overtime_minutes"] = {"from": rec.get("overtime_minutes", 0), "to": target_ot}

    if target_half_day != rec.get("half_day", False):
        mongo_changes["half_day"] = {"from": rec.get("half_day", False), "to": target_half_day}

    if not mongo_changes:
        raise HTTPException(status_code=422, detail="Request changes nothing")

    history_entry = {
        "at": datetime.now(timezone.utc),
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": mongo_changes,
    }

    # Optimistic concurrency matching history state
    current_history = rec.get("history", [])
    updated = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "history": current_history},
        {
            "$set": {
                "status": target_status,
                "punch_in": ms_to_dt(target_in_ms),
                "punch_out": ms_to_dt(target_out_ms),
                "work_hours": target_work_hours,
                "late_minutes": target_late,
                "overtime_minutes": target_ot,
                "half_day": target_half_day,
            },
            "$push": {"history": history_entry},
        },
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="Concurrent regularization conflict")

    return format_attendance_doc(updated)


# --------------------------------------------------------------------------- #
# Analytics Endpoints (Aggregation Pipelines)
# --------------------------------------------------------------------------- #
@app.get("/analytics/employees/{emp_code}/monthly", tags=["analytics"])
def employee_monthly(
    emp_code: str = Path(...),
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
):
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    year, m = map(int, month.split("-"))
    last_day = calendar.monthrange(year, m)[1]
    start_date = f"{month}-01"
    end_date = f"{month}-{last_day:02d}"

    working_days = get_working_days(month, emp.get("joined_on"))

    pipeline = [
        {"$match": {
            "emp_code": emp_code,
            "date": {"$gte": start_date, "$lte": end_date}
        }},
        {"$project": {
            "status": 1,
            "half_day": {"$ifNull": ["$half_day", False]},
            "late_minutes": {"$ifNull": ["$late_minutes", 0]},
            "overtime_minutes": {"$ifNull": ["$overtime_minutes", 0]},
            "dayOfWeek": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}},
        }},
        {"$project": {
            "is_weekday": {"$in": ["$dayOfWeek", [2, 3, 4, 5, 6]]},
            "is_present_status": {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
            "half_day": 1,
            "is_leave": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]},
            "late_minutes": 1,
            "overtime_minutes": 1,
        }},
        {"$group": {
            "_id": None,
            "present_days": {
                "$sum": {
                    "$cond": [
                        {"$and": ["$is_weekday", "$is_present_status"]},
                        {"$cond": ["$half_day", 0.5, 1.0]},
                        0.0
                    ]
                }
            },
            "leave_days": {"$sum": "$is_leave"},
            "late_count": {
                "$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}
            },
            "total_late_minutes": {"$sum": "$late_minutes"},
            "total_overtime_minutes": {"$sum": "$overtime_minutes"},
        }}
    ]

    agg_result = list(db.attendance_logs.aggregate(pipeline))
    if agg_result:
        row = agg_result[0]
        present_days = round_half_up_2(row.get("present_days", 0.0))
        leave_days = int(row.get("leave_days", 0))
        late_count = int(row.get("late_count", 0))
        total_late = int(row.get("total_late_minutes", 0))
        total_ot = int(row.get("total_overtime_minutes", 0))
    else:
        present_days = 0.0
        leave_days = 0
        late_count = 0
        total_late = 0
        total_ot = 0

    if working_days > 0:
        attendance_pct = round_half_up_2((present_days / working_days) * 100)
    else:
        attendance_pct = None

    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present_days,
        "leave_days": leave_days,
        "late_count": late_count,
        "total_late_minutes": total_late,
        "total_overtime_minutes": total_ot,
        "attendance_pct": attendance_pct,
    }


@app.get("/analytics/departments/summary", tags=["analytics"])
def department_summary(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = None,
):
    year, m = map(int, month.split("-"))
    last_day = calendar.monthrange(year, m)[1]
    start_date = f"{month}-01"
    end_date = f"{month}-{last_day:02d}"

    emp_match = {"joined_on": {"$lte": end_date}}
    if department:
        emp_match["department"] = department

    pipeline = [
        {"$match": emp_match},
        {"$lookup": {
            "from": "attendance_logs",
            "let": {"emp_code": "$emp_code"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$emp_code", "$$emp_code"]},
                            {"$gte": ["$date", start_date]},
                            {"$lte": ["$date", end_date]},
                        ]
                    }
                }},
                {"$project": {
                    "status": 1,
                    "work_hours": 1,
                    "half_day": {"$ifNull": ["$half_day", False]},
                    "late_minutes": {"$ifNull": ["$late_minutes", 0]},
                    "dayOfWeek": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}},
                }}
            ],
            "as": "logs"
        }},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "all_logs": {"$push": "$logs"}
        }},
        {"$project": {
            "department": "$_id",
            "headcount": 1,
            "flattened_logs": {
                "$reduce": {
                    "input": "$all_logs",
                    "initialValue": [],
                    "in": {"$concatArrays": ["$$value", "$$this"]}
                }
            }
        }},
        {"$project": {
            "_id": 0,
            "department": 1,
            "headcount": 1,
            "present_days": {
                "$sum": {
                    "$map": {
                        "input": "$flattened_logs",
                        "as": "log",
                        "in": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$in": ["$$log.dayOfWeek", [2, 3, 4, 5, 6]]},
                                        {"$in": ["$$log.status", ["PRESENT", "WFH", "ON_DUTY"]]}
                                    ]
                                },
                                {"$cond": ["$$log.half_day", 0.5, 1.0]},
                                0.0
                            ]
                        }
                    }
                }
            },
            "valid_work_hours": {
                "$filter": {
                    "input": "$flattened_logs",
                    "as": "log",
                    "cond": {
                        "$and": [
                            {"$in": ["$$log.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                            {"$ne": ["$$log.work_hours", None]}
                        ]
                    }
                }
            },
            "late_count": {
                "$sum": {
                    "$map": {
                        "input": "$flattened_logs",
                        "as": "log",
                        "in": {"$cond": [{"$gt": ["$$log.late_minutes", 0]}, 1, 0]}
                    }
                }
            },
            "total_late_minutes": {
                "$sum": "$flattened_logs.late_minutes"
            },
            "leave_count": {
                "$sum": {
                    "$map": {
                        "input": "$flattened_logs",
                        "as": "log",
                        "in": {"$cond": [{"$eq": ["$$log.status", "LEAVE"]}, 1, 0]}
                    }
                }
            },
            "on_duty_count": {
                "$sum": {
                    "$map": {
                        "input": "$flattened_logs",
                        "as": "log",
                        "in": {"$cond": [{"$eq": ["$$log.status", "ON_DUTY"]}, 1, 0]}
                    }
                }
            }
        }},
        {"$project": {
            "department": 1,
            "headcount": 1,
            "present_days": 1,
            "avg_work_hours": {
                "$cond": [
                    {"$gt": [{"$size": "$valid_work_hours"}, 0]},
                    {"$avg": "$valid_work_hours.work_hours"},
                    None
                ]
            },
            "late_count": 1,
            "total_late_minutes": 1,
            "leave_count": 1,
            "on_duty_count": 1
        }},
        {"$sort": {"department": 1}}
    ]

    raw_items = list(db.employees.aggregate(pipeline))
    items = []
    for row in raw_items:
        if row["headcount"] == 0:
            continue
        items.append({
            "department": row["department"],
            "headcount": int(row["headcount"]),
            "present_days": round_half_up_2(row.get("present_days", 0.0)),
            "avg_work_hours": round_half_up_2(row.get("avg_work_hours")),
            "late_count": int(row.get("late_count", 0)),
            "total_late_minutes": int(row.get("total_late_minutes", 0)),
            "leave_count": int(row.get("leave_count", 0)),
            "on_duty_count": int(row.get("on_duty_count", 0)),
        })

    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late", tags=["analytics"])
def late_leaderboard(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    year, m = map(int, month.split("-"))
    last_day = calendar.monthrange(year, m)[1]
    start_date = f"{month}-01"
    end_date = f"{month}-{last_day:02d}"

    pipeline = [
        {"$match": {
            "date": {"$gte": start_date, "$lte": end_date},
            "late_minutes": {"$gt": 0}
        }},
        {"$group": {
            "_id": "$emp_code",
            "total_late_minutes": {"$sum": "$late_minutes"},
            "late_count": {"$sum": 1}
        }},
        {"$lookup": {
            "from": "employees",
            "localField": "_id",
            "foreignField": "emp_code",
            "as": "emp"
        }},
        {"$unwind": "$emp"},
    ]

    if department:
        pipeline.append({"$match": {"emp.department": department}})

    # Standard competition ranking: ties share rank, next rank skipped (1, 2, 2, 4)
    pipeline.extend([
        {"$setWindowFields": {
            "sortBy": {"total_late_minutes": -1},
            "output": {
                "rank": {"$rank": {}}
            }
        }},
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {
            "_id": 0,
            "rank": 1,
            "emp_code": "$_id",
            "name": "$emp.name",
            "department": "$emp.department",
            "total_late_minutes": 1,
            "late_count": 1
        }}
    ])

    items = list(db.attendance_logs.aggregate(pipeline))
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend", tags=["analytics"])
def department_trend(
    department: str = Path(...),
    from_date: str = Query(..., alias="from", pattern=r"^\d{4}-\d{2}-\d{2}$"),
    to_date: str = Query(..., alias="to", pattern=r"^\d{4}-\d{2}-\d{2}$"),
):
    if from_date > to_date:
        raise HTTPException(status_code=422, detail="'to' must be greater than or equal to 'from'")

    dt_from = datetime.fromisoformat(from_date).date()
    dt_to = datetime.fromisoformat(to_date).date()
    day_count = (dt_to - dt_from).days + 1
    if day_count > 92:
        raise HTTPException(status_code=422, detail="Date range cannot exceed 92 days")

    dept_emp_count = db.employees.count_documents({"department": department})
    if dept_emp_count == 0:
        raise HTTPException(status_code=404, detail="Department not found")

    # Gap-filling daily series using $densify and 7-day moving average using $setWindowFields
    pipeline = [
        {"$match": {"department": department}},
        {"$limit": 1},
        {"$project": {
            "_id": 0,
            "d": datetime(dt_from.year, dt_from.month, dt_from.day)
        }},
        {"$densify": {
            "field": "d",
            "range": {
                "step": 1,
                "unit": "day",
                "bounds": [
                    datetime(dt_from.year, dt_from.month, dt_from.day),
                    datetime(dt_to.year, dt_to.month, dt_to.day) + timedelta(days=1)
                ]
            }
        }},
        {"$project": {
            "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$d"}},
            "is_working_day": {"$in": [{"$dayOfWeek": "$d"}, [2, 3, 4, 5, 6]]}
        }},
        {"$lookup": {
            "from": "employees",
            "let": {"cur_date": "$date"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$department", department]},
                            {"$lte": ["$joined_on", "$$cur_date"]}
                        ]
                    }
                }},
                {"$count": "count"}
            ],
            "as": "hc_res"
        }},
        {"$lookup": {
            "from": "attendance_logs",
            "let": {"cur_date": "$date"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$date", "$$cur_date"]}
                }},
                {"$lookup": {
                    "from": "employees",
                    "localField": "emp_code",
                    "foreignField": "emp_code",
                    "as": "emp"
                }},
                {"$unwind": "$emp"},
                {"$match": {"emp.department": department}},
                {"$group": {
                    "_id": None,
                    "present_count": {
                        "$sum": {
                            "$cond": [
                                {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                {"$cond": [{"$ifNull": ["$half_day", False]}, 0.5, 1.0]},
                                0.0
                            ]
                        }
                    },
                    "late_count": {
                        "$sum": {
                            "$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]
                        }
                    }
                }}
            ],
            "as": "log_res"
        }},
        {"$project": {
            "date": 1,
            "is_working_day": 1,
            "headcount": {
                "$ifNull": [{"$arrayElemAt": ["$hc_res.count", 0]}, 0]
            },
            "present_count": {
                "$ifNull": [{"$arrayElemAt": ["$log_res.present_count", 0]}, 0.0]
            },
            "late_count": {
                "$ifNull": [{"$arrayElemAt": ["$log_res.late_count", 0]}, 0]
            }
        }},
        {"$project": {
            "date": 1,
            "is_working_day": 1,
            "headcount": 1,
            "present_count": 1,
            "late_count": 1,
            "attendance_rate": {
                "$cond": [
                    {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                    {"$divide": ["$present_count", "$headcount"]},
                    None
                ]
            }
        }},
        {"$setWindowFields": {
            "sortBy": {"date": 1},
            "output": {
                "moving_avg_7d": {
                    "$avg": "$attendance_rate",
                    "window": {"documents": [-6, "current"]}
                }
            }
        }},
        {"$sort": {"date": 1}}
    ]

    raw_items = list(db.employees.aggregate(pipeline))
    items = []
    for r in raw_items:
        items.append({
            "date": r["date"],
            "is_working_day": r["is_working_day"],
            "headcount": int(r["headcount"]),
            "present_count": round_half_up_2(r["present_count"]),
            "late_count": int(r["late_count"]),
            "attendance_rate": round_half_up_4(r["attendance_rate"]),
            "moving_avg_7d": round_half_up_4(r["moving_avg_7d"]),
        })

    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# Admin Explain Endpoint
# --------------------------------------------------------------------------- #
@app.get("/admin/explain/{endpoint}", tags=["admin"])
def explain_endpoint(
    endpoint: str = Path(..., pattern=r"^(attendance_list|employee_monthly|department_summary|late_leaderboard|department_trend)$"),
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    date_to: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    status: Optional[str] = Query(None, pattern=r"^(PRESENT|ABSENT|LEAVE|WFH|ON_DUTY)$"),
    from_date: Optional[str] = Query(None, alias="from", pattern=r"^\d{4}-\d{2}-\d{2}$"),
    to_date: Optional[str] = Query(None, alias="to", pattern=r"^\d{4}-\d{2}-\d{2}$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    if endpoint == "attendance_list":
        q = {}
        if emp_code:
            q["emp_code"] = emp_code
        if date_from or date_to:
            q["date"] = {}
            if date_from:
                q["date"]["$gte"] = date_from
            if date_to:
                q["date"]["$lte"] = date_to
        if status:
            q["status"] = status
        skip = (page - 1) * page_size
        cursor = db.attendance_logs.find(q).sort([("date", -1), ("emp_code", 1)]).skip(skip).limit(page_size)
        explain_data = cursor.explain()
        clean_explain = json.loads(json_util.dumps(explain_data))
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": clean_explain}

    elif endpoint == "employee_monthly":
        if not emp_code or not month:
            raise HTTPException(status_code=422, detail="emp_code and month are required for employee_monthly")
        year, m = map(int, month.split("-"))
        last_day = calendar.monthrange(year, m)[1]
        start_date = f"{month}-01"
        end_date = f"{month}-{last_day:02d}"

        pipeline = [
            {"$match": {
                "emp_code": emp_code,
                "date": {"$gte": start_date, "$lte": end_date}
            }},
            {"$group": {"_id": None, "count": {"$sum": 1}}}
        ]
        explain_data = db.command("aggregate", "attendance_logs", pipeline=pipeline, explain=True)
        clean_explain = json.loads(json_util.dumps(explain_data))
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": clean_explain}

    elif endpoint == "department_summary":
        if not month:
            raise HTTPException(status_code=422, detail="month is required for department_summary")
        year, m = map(int, month.split("-"))
        last_day = calendar.monthrange(year, m)[1]
        end_date = f"{month}-{last_day:02d}"

        emp_match = {"joined_on": {"$lte": end_date}}
        if department:
            emp_match["department"] = department

        pipeline = [
            {"$match": emp_match},
            {"$lookup": {
                "from": "attendance_logs",
                "let": {"emp_code": "$emp_code"},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$emp_code", "$$emp_code"]},
                                {"$gte": ["$date", f"{month}-01"]},
                                {"$lte": ["$date", end_date]}
                            ]
                        }
                    }}
                ],
                "as": "logs"
            }}
        ]
        explain_data = db.command("aggregate", "employees", pipeline=pipeline, explain=True)
        clean_explain = json.loads(json_util.dumps(explain_data))
        return {"endpoint": endpoint, "collection": "employees", "explain": clean_explain}

    elif endpoint == "late_leaderboard":
        if not month:
            raise HTTPException(status_code=422, detail="month is required for late_leaderboard")
        year, m = map(int, month.split("-"))
        last_day = calendar.monthrange(year, m)[1]
        start_date = f"{month}-01"
        end_date = f"{month}-{last_day:02d}"

        pipeline = [
            {"$match": {
                "date": {"$gte": start_date, "$lte": end_date},
                "late_minutes": {"$gt": 0}
            }},
            {"$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": "$late_minutes"},
                "late_count": {"$sum": 1}
            }}
        ]
        explain_data = db.command("aggregate", "attendance_logs", pipeline=pipeline, explain=True)
        clean_explain = json.loads(json_util.dumps(explain_data))
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": clean_explain}

    elif endpoint == "department_trend":
        if not department or not from_date or not to_date:
            raise HTTPException(status_code=422, detail="department, from, and to are required for department_trend")
        pipeline = [
            {"$match": {"date": {"$gte": from_date, "$lte": to_date}}},
            {"$lookup": {
                "from": "employees",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "as": "emp"
            }},
            {"$unwind": "$emp"},
            {"$match": {"emp.department": department}}
        ]
        explain_data = db.command("aggregate", "attendance_logs", pipeline=pipeline, explain=True)
        clean_explain = json.loads(json_util.dumps(explain_data))
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": clean_explain}
