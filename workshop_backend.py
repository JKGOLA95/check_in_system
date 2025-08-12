# main.py - FastAPI Backend
from fastapi import FastAPI, HTTPException, UploadFile, File, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from typing import List, Optional
import pandas as pd
import qrcode
import io
import base64
import uuid
from datetime import datetime
import asyncpg
import asyncio
from contextlib import asynccontextmanager

# ---- SETTINGS ----
DATABASE_URL = "postgresql://postgres:41421041.exe@127.0.0.1:5432/workshop_db"
SEND_CONCURRENCY = 10  # limit parallel sends so providers aren't throttled

# ---- MODELS ----
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

# ---- DB POOL ----
db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=10, max_size=20)
    yield
    await db_pool.close()

app = FastAPI(title="Workshop Attendance API", lifespan=lifespan)

# ---- CORS ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- AUTH (simple) ----
security = HTTPBearer()
def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    # TODO: Implement JWT token verification (for now accept any non-empty bearer)
    if not credentials.token:
        raise HTTPException(status_code=401, detail="Invalid token")
    return credentials.token

# ---- CORE DB OPS ----
async def create_attendee(attendee: Attendee):
    attendee_id = str(uuid.uuid4())
    qr_data = f"WORKSHOP_ATTENDEE:{attendee_id}"

    # Generate QR image (base64 PNG)
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(qr_data)
    qr.make(fit=True)
    qr_image = qr.make_image(fill_color="black", back_color="white")
    img_buffer = io.BytesIO()
    qr_image.save(img_buffer, format='PNG')
    qr_base64 = base64.b64encode(img_buffer.getvalue()).decode()

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO attendees (id, name, email, mobile, batch, qr_code, qr_data, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            attendee_id, attendee.name, attendee.email, attendee.mobile,
            attendee.batch, qr_base64, qr_data, datetime.now()
        )

    return {
        "attendee_id": attendee_id,
        "qr_code": qr_base64,
        "qr_data": qr_data
    }

# ---- SENDING (STATUS + CONCURRENCY) ----
async def send_qr_code(attendee_id: str, attendee: Attendee, qr_code_base64: str) -> bool:
    """
    Send QR code via email and WhatsApp (your existing commented code kept below).
    Always update DB with qr_email_status / qr_whatsapp_status / qr_sent_at / qr_last_error.
    """
    email_ok = False
    wa_ok = False
    last_err = None
    now_ts = datetime.now()

    try:
        # EMAIL INTEGRATION - UPDATE WITH YOUR CREDENTIALS
        # import smtplib
        # from email.mime.multipart import MIMEMultipart
        # from email.mime.text import MIMEText
        # from email.mime.image import MIMEImage
        #
        # EMAIL_HOST = "smtp.gmail.com"  # UPDATE WITH YOUR SMTP HOST
        # EMAIL_PORT = 587
        # EMAIL_USER = "your-email@gmail.com"  # UPDATE WITH YOUR EMAIL
        # EMAIL_PASSWORD = "your-app-password"  # UPDATE WITH YOUR APP PASSWORD
        #
        # msg = MIMEMultipart()
        # msg['From'] = EMAIL_USER
        # msg['To'] = attendee.email
        # msg['Subject'] = f"Workshop QR Code - {attendee.batch}"
        #
        # body = f"""
        # Dear {attendee.name},
        #
        # Your registration for the workshop is confirmed!
        # Batch: {attendee.batch}
        #
        # Please find your QR code attached. Show this QR code at the entrance.
        #
        # Thank you!
        # """
        # msg.attach(MIMEText(body, 'plain'))
        #
        # # Attach QR code image
        # qr_image_data = base64.b64decode(qr_code_base64)
        # qr_attachment = MIMEImage(qr_image_data)
        # qr_attachment.add_header('Content-Disposition', 'attachment', filename='qr_code.png')
        # msg.attach(qr_attachment)
        #
        # server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        # server.starttls()
        # server.login(EMAIL_USER, EMAIL_PASSWORD)
        # server.send_message(msg)
        # server.quit()
        #
        # email_ok = True

        # WHATSAPP INTEGRATION - UPDATE WITH YOUR TWILIO CREDENTIALS
        # from twilio.rest import Client
        #
        # TWILIO_ACCOUNT_SID = "your_twilio_account_sid"  # UPDATE WITH YOUR SID
        # TWILIO_AUTH_TOKEN = "your_twilio_auth_token"    # UPDATE WITH YOUR TOKEN
        # TWILIO_WHATSAPP_NUMBER = "whatsapp:+14155238886"  # UPDATE WITH YOUR WHATSAPP NUMBER
        #
        # client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        #
        # # NOTE: Twilio WhatsApp prefers a public HTTPS media URL instead of data: URI.
        # # For quick start you can send just text without media_url, or host image publicly.
        # message = client.messages.create(
        #     body=f"Hello {attendee.name}! Your workshop registration is confirmed for {attendee.batch}. QR attached via email.",
        #     from_=TWILIO_WHATSAPP_NUMBER,
        #     to=f"whatsapp:+{attendee.mobile}",
        #     # media_url=[f"data:image/png;base64,{qr_code_base64}"]  # not ideal for WhatsApp
        # )
        #
        # wa_ok = True

        # ---- TEMP while third-party is commented out: mark as sent ----
        email_ok = True
        wa_ok = True

    except Exception as e:
        last_err = str(e)

    # Status update in DB (do not fail request if this write fails)
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE attendees
                   SET qr_email_status    = $2,
                       qr_whatsapp_status = $3,
                       qr_sent_at         = $4,
                       qr_last_error      = $5,
                       updated_at         = NOW()
                 WHERE id = $1
            """, attendee_id,
                 'sent' if email_ok else 'failed',
                 'sent' if wa_ok else 'failed',
                 now_ts,
                 last_err)
    except Exception as e:
        print("[WARN] status write failed:", e)

    return email_ok and wa_ok

async def send_entry_pass(attendee_data, entry_time):
    """Send entry pass confirmation (kept simple; plug providers when ready)."""
    try:
        # EMAIL INTEGRATION FOR ENTRY PASS
        # WHATSAPP INTEGRATION FOR ENTRY PASS
        # (Same pattern as send_qr_code)
        print(f"Entry pass sent to {attendee_data['name']}")
        return True
    except Exception as e:
        print(f"Error sending entry pass: {str(e)}")
        return False

# ---- API ENDPOINTS ----

@app.post("/api/register/bulk")
async def bulk_register(attendees: BulkAttendee, token: str = Depends(verify_token)):
    """
    Bulk registration of attendees
    - Create all attendees (sequentially)
    - Send QR with concurrency cap (no batching needed)
    - Status tracked in DB
    """
    created = []
    for attendee in attendees.attendees:
        res = await create_attendee(attendee)
        created.append((attendee, res["attendee_id"], res["qr_code"]))

    # cap parallel sends
    sem = asyncio.Semaphore(SEND_CONCURRENCY)

    async def _send_one(a: Attendee, aid: str, qr: str):
        async with sem:
            await send_qr_code(aid, a, qr)

    await asyncio.gather(*(_send_one(a, aid, qr) for (a, aid, qr) in created))

    # response payload
    data = [
        {
            "name": a.name, "email": a.email, "mobile": a.mobile,
            "batch": a.batch, "attendee_id": aid, "qr_code": qr
        }
        for (a, aid, qr) in created
    ]
    return {"message": f"Successfully registered {len(created)} attendees", "data": data}

@app.post("/api/register/single")
async def single_register(attendee: Attendee, token: str = Depends(verify_token)):
    """Single attendee registration"""
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
    """QR code scanning and attendance marking"""
    try:
        async with db_pool.acquire() as conn:
            attendee = await conn.fetchrow(
                "SELECT * FROM attendees WHERE qr_data = $1",
                scan_data.qr_code
            )
            if not attendee:
                raise HTTPException(status_code=404, detail="Invalid QR code")

            existing_attendance = await conn.fetchrow(
                "SELECT * FROM attendance WHERE attendee_id = $1",
                attendee['id']
            )
            if existing_attendance:
                return {
                    "message": "Attendance already marked",
                    "attendee": {
                        "name": attendee['name'],
                        "batch": attendee['batch'],
                        "entry_time": existing_attendance['entry_time']
                    }
                }

            entry_time = datetime.now()
            await conn.execute(
                """
                INSERT INTO attendance (attendee_id, entry_time, created_at)
                VALUES ($1, $2, $3)
                """,
                attendee['id'], entry_time, entry_time
            )

        await send_entry_pass(attendee, entry_time)

        return {
            "message": "Attendance marked successfully",
            "attendee": {
                "name": attendee['name'],
                "email": attendee['email'],
                "mobile": attendee['mobile'],
                "batch": attendee['batch'],
                "entry_time": entry_time
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing QR scan: {str(e)}")

@app.get("/api/attendance/dashboard")
async def get_attendance_dashboard(token: str = Depends(verify_token)):
    """Get attendance dashboard data"""
    async with db_pool.acquire() as conn:
        total_attendees = await conn.fetchval("SELECT COUNT(*) FROM attendees")
        marked_attendance = await conn.fetchval("SELECT COUNT(*) FROM attendance")
        batch_data = await conn.fetch(
            """
            SELECT a.batch, 
                   COUNT(a.id) as total_registered,
                   COUNT(att.id) as total_attended
            FROM attendees a
            LEFT JOIN attendance att ON a.id = att.attendee_id
            GROUP BY a.batch
            """
        )
    return {
        "total_attendees": total_attendees,
        "marked_attendance": marked_attendance,
        "attendance_rate": round((marked_attendance / total_attendees) * 100, 2) if total_attendees > 0 else 0,
        "batch_wise_data": [dict(row) for row in batch_data]
    }

@app.post("/api/upload/csv")
async def upload_csv(file: UploadFile = File(...), token: str = Depends(verify_token)):
    """Upload CSV file for bulk registration (concurrency-capped sending)"""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    try:
        content = await file.read()
        # BOM-safe for Excel CSVs
        df = pd.read_csv(io.StringIO(content.decode('utf-8-sig')))

        required_columns = ['name', 'email', 'mobile', 'batch']
        if not all(col in df.columns for col in required_columns):
            raise HTTPException(
                status_code=400,
                detail=f"CSV must contain columns: {required_columns}"
            )

        attendees: List[Attendee] = []
        for _, row in df.iterrows():
            attendees.append(Attendee(
                name=str(row['name']).strip(),
                email=EmailStr(str(row['email']).strip()),
                mobile=str(row['mobile']).strip(),
                batch=str(row['batch']).strip()
            ))

        # 1) create all
        created = []
        for a in attendees:
            res = await create_attendee(a)
            created.append((a, res["attendee_id"], res["qr_code"]))

        # 2) send with concurrency cap
        sem = asyncio.Semaphore(SEND_CONCURRENCY)
        async def _send_one(a: Attendee, aid: str, qr: str):
            async with sem:
                await send_qr_code(aid, a, qr)

        await asyncio.gather(*(_send_one(a, aid, qr) for (a, aid, qr) in created))

        return {
            "message": f"Successfully processed {len(created)} attendees from CSV",
            "total_processed": len(created)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing CSV: {str(e)}")

@app.post("/api/resend/pending")
async def resend_pending(limit: int = 200, token: str = Depends(verify_token)):
    """
    Retry send for those not marked 'sent' on email or WhatsApp.
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name, email, mobile, batch, qr_code
              FROM attendees
             WHERE (qr_email_status <> 'sent' OR qr_whatsapp_status <> 'sent')
             ORDER BY created_at
             LIMIT $1
            """,
            limit
        )

    sem = asyncio.Semaphore(SEND_CONCURRENCY)

    async def _resend(r):
        a = Attendee(name=r['name'], email=r['email'], mobile=r['mobile'], batch=r['batch'])
        async with sem:
            return await send_qr_code(r['id'], a, r['qr_code'])

    results = await asyncio.gather(*(_resend(r) for r in rows), return_exceptions=True)
    ok = sum(1 for r in results if r is True)
    return {"retried": len(rows), "success": ok, "failed": len(rows) - ok}

# Dev runner
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
