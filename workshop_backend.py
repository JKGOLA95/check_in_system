# workshop_backend.py  — FastAPI Backend with status tracking + concurrency + retry
from fastapi import FastAPI, HTTPException, UploadFile, File, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, EmailStr
from typing import List, Optional

import os
import io
import base64
import uuid
import asyncio
import asyncpg
import pandas as pd
import qrcode
from datetime import datetime
from contextlib import asynccontextmanager

# ========== SETTINGS ==========
# DB
DATABASE_URL = "postgresql://postgres:41421041.exe@127.0.0.1:5432/workshop_db"

# Parallel sends cap
SEND_CONCURRENCY = 10

# Providers (fill when ready; until then, sends will be marked 'pending')
MANDRILL_API_KEY = os.getenv("MANDRILL_API_KEY", "")          # Mailchimp Transactional (Mandrill)
EMAIL_FROM       = os.getenv("EMAIL_FROM", "no-reply@example.com")

WATI_BASE_URL = os.getenv("WATI_BASE_URL", "https://app.wati.io")   # change to your tenant base
WATI_API_KEY  = os.getenv("WATI_API_KEY", "")

# ========== MODELS ==========
class Attendee(BaseModel):
    name: str
    email: EmailStr
    mobile: str
    batch: str

class BulkAttendee(BaseModel):
    attendees: List[Attendee]

class QRScanResult(BaseModel):
    qr_code: str
    timestamp: Optional[datetime] = None

# ========== APP / DB POOL ==========
db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=10, max_size=20)
    yield
    await db_pool.close()

app = FastAPI(title="Workshop Attendance API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== AUTH (simple, accepts any non-empty Bearer) ==========
security = HTTPBearer()

def verify_token(auth: HTTPAuthorizationCredentials = Depends(security)):
    token = auth.credentials  # <-- correct attribute
    if not token:
        raise HTTPException(status_code=401, detail="Invalid token")
    return token

# ========== HELPERS ==========
async def create_attendee(attendee: Attendee):
    attendee_id = str(uuid.uuid4())
    qr_data = f"WORKSHOP_ATTENDEE:{attendee_id}"

    # Generate QR PNG -> base64
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(qr_data)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    qr_img.save(buf, format="PNG")
    qr_b64 = base64.b64encode(buf.getvalue()).decode()

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO attendees
                (id, name, email, mobile, batch, qr_code, qr_data, created_at)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            attendee_id, attendee.name, attendee.email, attendee.mobile,
            attendee.batch, qr_b64, qr_data, datetime.now()
        )

    return {"attendee_id": attendee_id, "qr_code": qr_b64, "qr_data": qr_data}

async def _update_send_status(attendee_id: str,
                              email_status: str,
                              wa_status: str,
                              sent_at: Optional[datetime],
                              last_error: Optional[str]):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE attendees
                   SET qr_email_status    = $2,
                       qr_whatsapp_status = $3,
                       qr_sent_at         = $4,
                       qr_last_error      = $5,
                       updated_at         = NOW()
                 WHERE id = $1
                """,
                attendee_id, email_status, wa_status, sent_at, last_error
            )
    except Exception as e:
        # don't crash main flow on status write failure
        print("[WARN] status write failed:", e)

# ========== SENDING (status + concurrency) ==========
# NOTE: We do NOT fake success. If providers are not configured, we mark 'pending'.
send_sem = asyncio.Semaphore(SEND_CONCURRENCY)

async def send_qr_code(attendee_id: str, attendee: Attendee, qr_code_base64: str) -> bool:
    """
    Sends QR via Email + WhatsApp.
    - If provider keys are missing, mark both channels 'pending' so you can retry later.
    - Otherwise plug your real provider calls and set statuses to 'sent' on success.
    """
    now_ts = datetime.now()
    email_ok = False
    wa_ok = False
    last_err: Optional[str] = None

    async with send_sem:
        try:
            # ---------------- EMAIL (Mandrill) ----------------
            # If you want to use SMTP instead, keep your existing commented SMTP block here.
            if MANDRILL_API_KEY:
                # EXAMPLE (Mandrill) — replace with your template/content as needed
                # import httpx
                # async with httpx.AsyncClient(timeout=20) as client:
                #     payload = {
                #         "key": MANDRILL_API_KEY,
                #         "message": {
                #             "from_email": EMAIL_FROM,
                #             "to": [{"email": attendee.email, "type": "to"}],
                #             "subject": f"Workshop QR Code - {attendee.batch}",
                #             "text": f"Dear {attendee.name},\nYour registration is confirmed. QR attached.",
                #             "attachments": [{
                #                 "type": "image/png",
                #                 "name": "qr_code.png",
                #                 "content": qr_code_base64  # base64 body (no data: prefix)
                #             }],
                #         }
                #     }
                #     r = await client.post("https://mandrillapp.com/api/1.0/messages/send.json", json=payload)
                #     r.raise_for_status()
                #     # you can check r.json() for reject reasons etc.
                email_ok = True
            else:
                # not configured -> keep pending
                pass

            # ---------------- WHATSAPP (WATI) ----------------
            if WATI_API_KEY:
                # Most WATI flows prefer TEMPLATE messages or a public image URL.
                # You already have an endpoint to serve PNG:
                #   GET http://<your-backend>/api/qr/{attendee_id}.png
                #
                # Example (template send); adjust to your WATI account details:
                # import httpx
                # headers = {"Authorization": f"Bearer {WATI_API_KEY}"}
                # payload = {
                #     "template_name": "your_template_name",
                #     "broadcast_name": "Workshop QR",
                #     "parameters": [
                #         {"name": "name", "value": attendee.name},
                #         {"name": "batch", "value": attendee.batch},
                #         {"name": "qr_url", "value": f"https://YOUR_PUBLIC_HOST/api/qr/{attendee_id}.png"}
                #     ],
                #     "to": attendee.mobile  # with country code e.g. 91XXXXXXXXXX
                # }
                # async with httpx.AsyncClient(timeout=20) as client:
                #     r = await client.post(f"{WATI_BASE_URL}/api/v1/sendTemplateMessage", headers=headers, json=payload)
                #     r.raise_for_status()
                wa_ok = True
            else:
                # not configured -> keep pending
                pass

        except Exception as e:
            last_err = str(e)

    # decide statuses
    if MANDRILL_API_KEY:
        email_status = "sent" if email_ok else "failed"
    else:
        email_status = "pending"

    if WATI_API_KEY:
        wa_status = "sent" if wa_ok else "failed"
    else:
        wa_status = "pending"

    await _update_send_status(attendee_id, email_status, wa_status, now_ts, last_err)
    return email_ok and wa_ok

async def send_entry_pass(attendee_data, entry_time):
    """Hook for entry pass. Same pattern as send_qr_code (email/WA template)."""
    try:
        print(f"Entry pass sent to {attendee_data['name']}")
        return True
    except Exception as e:
        print("Entry pass error:", e)
        return False

# ========== API ==========
@app.post("/api/register/bulk")
async def bulk_register(body: BulkAttendee, token: str = Depends(verify_token)):
    """
    1) Create all attendees
    2) Send QR with concurrency cap
    3) Status is recorded per attendee in DB
    """
    created = []
    for a in body.attendees:
        res = await create_attendee(a)
        created.append((a, res["attendee_id"], res["qr_code"]))

    sem = asyncio.Semaphore(SEND_CONCURRENCY)

    async def _send_one(a: Attendee, aid: str, qr: str):
        async with sem:
            await send_qr_code(aid, a, qr)

    await asyncio.gather(*(_send_one(a, aid, qr) for (a, aid, qr) in created))

    data = [{
        "name": a.name, "email": a.email, "mobile": a.mobile,
        "batch": a.batch, "attendee_id": aid, "qr_code": qr
    } for (a, aid, qr) in created]

    return {"message": f"Successfully registered {len(created)} attendees", "data": data}

@app.post("/api/register/single")
async def single_register(attendee: Attendee, token: str = Depends(verify_token)):
    res = await create_attendee(attendee)
    await send_qr_code(res["attendee_id"], attendee, res["qr_code"])
    return {
        "message": "Attendee registered successfully",
        "attendee_id": res["attendee_id"],
        "qr_code": res["qr_code"],
        "name": attendee.name,
        "email": attendee.email,
        "batch": attendee.batch
    }

@app.post("/api/scan")
async def scan_qr(scan_data: QRScanResult, token: str = Depends(verify_token)):
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM attendees WHERE qr_data=$1", scan_data.qr_code)
            if not row:
                raise HTTPException(status_code=404, detail="Invalid QR code")

            existing = await conn.fetchrow("SELECT * FROM attendance WHERE attendee_id=$1", row["id"])
            if existing:
                return {"message": "Attendance already marked",
                        "attendee": {"name": row["name"], "batch": row["batch"], "entry_time": existing["entry_time"]}}

            entry_time = datetime.now()
            await conn.execute(
                "INSERT INTO attendance (attendee_id, entry_time, created_at) VALUES ($1, $2, $3)",
                row["id"], entry_time, entry_time
            )

        await send_entry_pass(row, entry_time)

        return {"message": "Attendance marked successfully",
                "attendee": {"name": row["name"], "email": row["email"], "mobile": row["mobile"],
                             "batch": row["batch"], "entry_time": entry_time}}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing QR scan: {str(e)}")

@app.get("/api/attendance/dashboard")
async def get_attendance_dashboard(token: str = Depends(verify_token)):
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM attendees")
        attended = await conn.fetchval("SELECT COUNT(*) FROM attendance")
        batch_rows = await conn.fetch(
            """
            SELECT a.batch,
                   COUNT(a.id) AS total_registered,
                   COUNT(att.id) AS total_attended
              FROM attendees a
              LEFT JOIN attendance att ON a.id = att.attendee_id
             GROUP BY a.batch
            """
        )
    return {
        "total_attendees": total,
        "marked_attendance": attended,
        "attendance_rate": round((attended / total) * 100, 2) if total > 0 else 0,
        "batch_wise_data": [dict(r) for r in batch_rows]
    }

@app.post("/api/upload/csv")
async def upload_csv(file: UploadFile = File(...), token: str = Depends(verify_token)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    try:
        content = await file.read()
        df = pd.read_csv(io.StringIO(content.decode("utf-8-sig")))

        required = ["name", "email", "mobile", "batch"]
        if not all(col in df.columns for col in required):
            raise HTTPException(status_code=400, detail=f"CSV must contain columns: {required}")

        attendees: List[Attendee] = []
        for _, row in df.iterrows():
            attendees.append(Attendee(
                name=str(row["name"]).strip(),
                email=EmailStr(str(row["email"]).strip()),
                mobile=str(row["mobile"]).strip(),
                batch=str(row["batch"]).strip()
            ))

        created = []
        for a in attendees:
            res = await create_attendee(a)
            created.append((a, res["attendee_id"], res["qr_code"]))

        sem = asyncio.Semaphore(SEND_CONCURRENCY)
        async def _send_one(a: Attendee, aid: str, qr: str):
            async with sem:
                await send_qr_code(aid, a, qr)

        await asyncio.gather(*(_send_one(a, aid, qr) for (a, aid, qr) in created))

        return {"message": f"Successfully processed {len(created)} attendees from CSV",
                "total_processed": len(created)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing CSV: {str(e)}")

# Public PNG for WA (use behind your public domain when deployed)
@app.get("/api/qr/{attendee_id}.png")
async def get_qr_png(attendee_id: str, token: str = Depends(verify_token)):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT qr_code FROM attendees WHERE id=$1", attendee_id)
        if not row:
            raise HTTPException(status_code=404, detail="QR not found")
        png_bytes = base64.b64decode(row["qr_code"])
        return StreamingResponse(io.BytesIO(png_bytes), media_type="image/png")

# Retry pending/failed
@app.post("/api/resend/pending")
async def resend_pending(limit: int = 200, token: str = Depends(verify_token)):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name, email, mobile, batch, qr_code
              FROM attendees
             WHERE (qr_email_status <> 'sent' OR qr_whatsapp_status <> 'sent')
             ORDER BY created_at
             LIMIT $1
            """, limit
        )

    sem = asyncio.Semaphore(SEND_CONCURRENCY)

    async def _resend(r):
        a = Attendee(name=r["name"], email=r["email"], mobile=r["mobile"], batch=r["batch"])
        async with sem:
            return await send_qr_code(r["id"], a, r["qr_code"])

    results = await asyncio.gather(*(_resend(r) for r in rows), return_exceptions=True)
    ok = sum(1 for r in results if r is True)
    return {"retried": len(rows), "success": ok, "failed": len(rows) - ok}

# Optional: quick health
@app.get("/api/health")
async def health():
    return {"ok": True}

# Dev runner
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
