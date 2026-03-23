import os
import json
import time
import logging
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime

logger = logging.getLogger("db")

# ── Firebase Init (Safe — from env variable) ────────────────────────────────
_db_client = None

def get_firebase() -> firestore.Client | None:
    global _db_client
    if _db_client:
        return _db_client
    try:
        cred_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON", "")
        if not cred_json:
            logger.warning("[DB] FIREBASE_SERVICE_ACCOUNT_JSON not set")
            return None
        cred_dict = json.loads(cred_json)
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
        _db_client = firestore.client()
        return _db_client
    except Exception as e:
        logger.error(f"[DB] Firebase init failed: {e}")
        return None

# ── get_supabase() — Backward Compat Alias ─────────────────────────────────
def get_supabase():
    """Alias for backward compatibility with existing code."""
    return get_firebase()

# ── save_call_log ────────────────────────────────────────────────────────────
def save_call_log(
    phone: str,
    duration: int,
    transcript: str,
    summary: str = "",
    recording_url: str = "",
    caller_name: str = "",
    sentiment: str = "unknown",
    estimated_cost_usd: float | None = None,
    call_date: str | None = None,
    call_hour: int | None = None,
    call_day_of_week: str | None = None,
    was_booked: bool = False,
    interrupt_count: int = 0,
) -> dict:
    """Insert a call log into Firebase Firestore."""
    db = get_firebase()
    if not db:
        logger.info(f"[DB] Firebase not configured. Local log → {phone}")
        return {"success": False, "message": "Firebase not configured"}
    try:
        data = {
            "phone_number": phone,
            "caller_name": caller_name,
            "duration_seconds": duration,
            "transcript": transcript,
            "summary": summary,
            "recording_url": recording_url,
            "sentiment": sentiment,
            "was_booked": was_booked,
            "interrupt_count": interrupt_count,
            "estimated_cost_usd": estimated_cost_usd,
            "call_date": call_date,
            "call_hour": call_hour,
            "call_day_of_week": call_day_of_week,
            "created_at": firestore.SERVER_TIMESTAMP,
        }
        db.collection("call_logs").add(data)
        logger.info(f"[DB] Call log saved for {phone}")
        return {"success": True}
    except Exception as e:
        logger.error(f"[DB] save_call_log failed: {e}")
        return {"success": False, "message": str(e)}

# ── get_config_from_firebase (Agent Config from Dashboard) ──────────────────
def get_config_from_firebase(client_id: str) -> dict | None:
    """
    Fetch agent config from Firebase.
    Dashboard saves to agent_configs/{client_id}
    Python worker reads from here.
    """
    db = get_firebase()
    if not db:
        return None
    try:
        doc = db.collection("agent_configs").document(client_id).get()
        if doc.exists:
            logger.info(f"[DB] Config loaded from Firebase for: {client_id}")
            return doc.to_dict()
        return None
    except Exception as e:
        logger.error(f"[DB] get_config_from_firebase failed: {e}")
        return None

# ── fetch_call_logs ──────────────────────────────────────────────────────────
def fetch_call_logs(limit: int = 50) -> list:
    """Fetch recent call logs from Firebase."""
    db = get_firebase()
    if not db:
        return []
    try:
        docs = (db.collection("call_logs")
                .order_by("created_at", direction=firestore.Query.DESCENDING)
                .limit(limit)
                .stream())
        return [{**d.to_dict(), "id": d.id} for d in docs]
    except Exception as e:
        logger.error(f"[DB] fetch_call_logs failed: {e}")
        return []

# ── fetch_bookings ───────────────────────────────────────────────────────────
def fetch_bookings() -> list:
    """Fetch calls where booking was confirmed."""
    db = get_firebase()
    if not db:
        return []
    try:
        docs = (db.collection("call_logs")
                .where("was_booked", "==", True)
                .order_by("created_at", direction=firestore.Query.DESCENDING)
                .limit(200)
                .stream())
        return [{**d.to_dict(), "id": d.id} for d in docs]
    except Exception as e:
        logger.error(f"[DB] fetch_bookings failed: {e}")
        return []

# ── fetch_stats ──────────────────────────────────────────────────────────────
def fetch_stats() -> dict:
    """Calculate aggregate stats from call logs."""
    _empty = {"total_calls": 0, "total_bookings": 0, "avg_duration": 0, "booking_rate": 0}
    db = get_firebase()
    if not db:
        return _empty
    try:
        docs = db.collection("call_logs").stream()
        rows = [d.to_dict() for d in docs]
        total = len(rows)
        bookings = sum(1 for r in rows if r.get("was_booked"))
        durations = [r["duration_seconds"] for r in rows if r.get("duration_seconds")]
        avg_dur = round(sum(durations) / len(durations)) if durations else 0
        rate = round((bookings / total) * 100) if total else 0
        return {"total_calls": total, "total_bookings": bookings,
                "avg_duration": avg_dur, "booking_rate": rate}
    except Exception as e:
        logger.error(f"[DB] fetch_stats failed: {e}")
        return _empty
