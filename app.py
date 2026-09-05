import os
import asyncio
import random
import time
import httpx
import psycopg2
import psycopg2.extras
import secrets
from typing import Optional
from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import uvicorn

app = FastAPI(title="Comment To DM Engine")

# =========================================================
# 🔴 META APP CREDENTIALS
# =========================================================
FB_APP_ID = "4540018746283778"
FB_APP_SECRET = "44b48575462c74e05dc2dae3b7c74886"
VERIFY_TOKEN = "hasan1235"
# =========================================================

# =========================================================
# 🔒 DASHBOARD SECURITY (ADMIN LOGIN)
# =========================================================
security = HTTPBasic()

# Change these to your own secret login details!
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "aazzxxcc321@"

def verify_admin(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, ADMIN_USERNAME)
    correct_password = secrets.compare_digest(credentials.password, ADMIN_PASSWORD)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized Access",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials
# =========================================================

DB_URL = os.environ.get("DATABASE_URL")
MAX_PER_HOUR = 700
message_queue = asyncio.Queue()
RATE_LIMIT_TRACKER = {}

# =========================================================
# 📝 RANDOMIZED UNIQUE COMMENT ENGINE (50 NEUTRAL REPLIES)
# =========================================================
PUBLIC_REPLIES_MASTER = [
    "Answer locked in! Follow for more.",
    "Thanks for guessing! Share to test a friend.",
    "Interesting take! Follow for daily puzzles.",
    "Love the participation! Tag a smart buddy.",
    "Guess received! Hit follow to play daily.",
    "Let's see if that's it! Share this post.",
    "Appreciate the comment! Follow to train your brain.",
    "Answer noted! Challenge a friend to try.",
    "Thanks for playing! Follow so you don't miss out.",
    "Good effort! Share if you love brain teasers.",
    "We have your answer! Hit follow for tomorrow's trivia.",
    "Fascinating guess! Tag someone who loves puzzles.",
    "Let's see how you did! Follow for more.",
    "Response recorded! Share to trick your friends.",
    "Love to see it! Follow us for daily challenges.",
    "That's one way to look at it! Tag a friend.",
    "Guess is in! Hit follow to keep playing.",
    "Thanks for joining in! Share with your family.",
    "Let's check that answer! Follow for more riddles.",
    "Got your response! Challenge someone today.",
    "Answer logged! Make sure to follow the page.",
    "Interesting thought! Share this brain teaser.",
    "Thanks for your guess! Tag someone to compete.",
    "We see your answer! Follow for daily questions.",
    "Let's find out! Share if you love trivia.",
    "Great to have you playing! Hit follow.",
    "Response locked! Challenge a coworker to solve this.",
    "Appreciate the try! Follow for tomorrow's brain buster.",
    "Guess noted! Share to stump the internet.",
    "Thanks for participating! Tag a friend to play.",
    "Answer is in! Follow to keep your mind sharp.",
    "Let's see if you cracked it! Share this post.",
    "Love the engagement! Follow us for more.",
    "Your guess is safe with us! Tag a puzzle lover.",
    "Thanks for commenting! Hit follow for the next one.",
    "Response received! Challenge a friend to beat you.",
    "We got it! Share if this made you think.",
    "Guess confirmed! Follow for daily Q&A.",
    "Let's see what happens! Tag someone smart.",
    "Answer submitted! Follow so you never miss a puzzle.",
    "Appreciate you playing! Share with your group chat.",
    "Got it! Hit follow for more brain training.",
    "Let's see if you're right! Challenge a buddy.",
    "Thanks for dropping an answer! Follow us.",
    "Response locked and loaded! Tag a friend.",
    "We see you! Share this if you love a challenge.",
    "Answer received loud and clear! Hit follow.",
    "Thanks for the guess! Follow for the next puzzle.",
    "Interesting answer! Tag a friend to see theirs.",
    "Guess is officially in! Share to test others."
]

AVAILABLE_REPLIES = []

def get_unique_reply():
    global AVAILABLE_REPLIES
    if not AVAILABLE_REPLIES:
        AVAILABLE_REPLIES = PUBLIC_REPLIES_MASTER.copy()
        random.shuffle(AVAILABLE_REPLIES)
    return AVAILABLE_REPLIES.pop()
# =========================================================

def get_base_url(request: Request):
    host = request.headers.get("x-forwarded-host", request.url.netloc)
    scheme = "http" if "localhost" in host or "127.0.0.1" in host else "https"
    return f"{scheme}://{host}"

# =========================================================
# 1. POSTGRES DATABASE INITIALIZATION
# =========================================================
def get_db():
    conn = psycopg2.connect(DB_URL)
    conn.cursor_factory = psycopg2.extras.DictCursor
    return conn

def init_db():
    if not DB_URL:
        print("⚠️ WARNING: No DATABASE_URL found. Please add it to Render Environment Variables.")
        return
        
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pages (
                    page_id TEXT PRIMARY KEY,
                    page_name TEXT NOT NULL,
                    access_token TEXT NOT NULL,
                    dms_opened INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS campaigns (
                    id SERIAL PRIMARY KEY,
                    page_id TEXT NOT NULL,
                    post_id TEXT NOT NULL,
                    campaign_name TEXT NOT NULL,
                    trigger_keywords TEXT NOT NULL,
                    dm_text TEXT NOT NULL,
                    button_text TEXT NOT NULL,
                    button_url TEXT NOT NULL,
                    dms_sent INTEGER DEFAULT 0,
                    dms_opened INTEGER DEFAULT 0,
                    link_clicks INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (page_id) REFERENCES pages (page_id),
                    UNIQUE(page_id, post_id)
                )
            """)
            cursor.execute("""
                ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS dms_opened INTEGER DEFAULT 0;
            """)
        conn.commit()

init_db()

# =========================================================
# 2. UPTIME KEEP-ALIVE ROUTE
# =========================================================
@app.get("/ping")
async def keep_alive():
    return PlainTextResponse("Bot is awake and running 24/7!", status_code=200)

# =========================================================
# 3. FACEBOOK OAUTH LOGIN
# =========================================================
@app.get("/auth/facebook")
async def auth_facebook(request: Request, credentials: HTTPBasicCredentials = Depends(verify_admin)):
    redirect_uri = get_base_url(request) + "/auth/callback"
    # Includes pages_manage_engagement for posting comment replies
    scopes = "pages_show_list,pages_read_engagement,pages_manage_engagement,pages_manage_posts,pages_messaging,pages_manage_metadata"
    auth_url = f"https://www.facebook.com/v19.0/dialog/oauth?client_id={FB_APP_ID}&redirect_uri={redirect_uri}&scope={scopes}"
    return RedirectResponse(auth_url)

@app.get("/auth/callback")
async def auth_callback(request: Request, code: str = None):
    if not code:
        return HTMLResponse("Authorization failed or denied by user.")

    redirect_uri = get_base_url(request) + "/auth/callback"
    
    async with httpx.AsyncClient() as client:
        token_url = "https://graph.facebook.com/v19.0/oauth/access_token"
        res = await client.get(token_url, params={
            "client_id": FB_APP_ID,
            "redirect_uri": redirect_uri,
            "client_secret": FB_APP_SECRET,
            "code": code
        })
        user_token = res.json().get("access_token")
        
        if not user_token:
            return HTMLResponse(f"Failed to get token: {res.text}")

        pages_url = "https://graph.facebook.com/v19.0/me/accounts"
        pages_res = await client.get(pages_url, params={"access_token": user_token})
        pages_data = pages_res.json().get("data", [])

        with get_db() as conn:
            with conn.cursor() as cursor:
                for page in pages_data:
                    try:
                        sub_res = await client.post(
                            f"https://graph.facebook.com/v19.0/{page['id']}/subscribed_apps",
                            params={"access_token": page["access_token"], "subscribed_fields": "feed,messages,message_reads"}
                        )
                        print(f"🔗 Page Subscription Result for {page['name']}: {sub_res.status_code} - {sub_res.text}")
                    except Exception as e:
                        print(f"❌ Page Subscription Exception: {e}")

                    cursor.execute("""
                        INSERT INTO pages (page_id, page_name, access_token) 
                        VALUES (%s, %s, %s)
                        ON CONFLICT (page_id) DO UPDATE SET 
                        page_name = EXCLUDED.page_name, 
                        access_token = EXCLUDED.access_token
                    """, (page["id"], page["name"], page["access_token"]))
            conn.commit()

    return RedirectResponse("/")

# =========================================================
# 4. LINK CLICK TRACKER
# =========================================================
@app.get("/click/{campaign_id}")
async def track_link_click(campaign_id: int):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT button_url FROM campaigns WHERE id = %s", (campaign_id,))
            row = cursor.fetchone()
            
            if row and row["button_url"]:
                cursor.execute("UPDATE campaigns SET link_clicks = link_clicks + 1 WHERE id = %s", (campaign_id,))
                conn.commit()
                return RedirectResponse(row["button_url"])
            
    return PlainTextResponse("Link expired or invalid.")

# =========================================================
# 5. DYNAMIC POST FETCHER
# =========================================================
@app.get("/api/posts/{page_id}")
async def get_page_posts(page_id: str, credentials: HTTPBasicCredentials = Depends(verify_admin)):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT access_token FROM pages WHERE page_id = %s", (page_id,))
            row = cursor.fetchone()
            if not row:
                return JSONResponse({"error": "Page not found"}, status_code=404)
            access_token = row["access_token"]
    
    async with httpx.AsyncClient() as client:
        url = f"https://graph.facebook.com/v19.0/{page_id}/published_posts"
        res = await client.get(url, params={"fields": "id,message,created_time,full_picture", "access_token": access_token, "limit": 15})
        return res.json()

# =========================================================
# 6. ASYNC BACKGROUND WORKER (PUBLIC REPLY + PRIVATE DM)
# =========================================================
async def process_queue():
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            job = await message_queue.get()
            page_id = job["page_id"]
            comment_id = job["comment_id"]
            sender_name = job["sender_name"]
            campaign = job["campaign"]
            token = job["token"]
            base_url = job["base_url"]

            curr_time = time.time()
            if page_id not in RATE_LIMIT_TRACKER:
                RATE_LIMIT_TRACKER[page_id] = {"count": 0, "reset_time": curr_time}
            if curr_time - RATE_LIMIT_TRACKER[page_id]["reset_time"] > 3600:
                RATE_LIMIT_TRACKER[page_id] = {"count": 0, "reset_time": curr_time}
            if RATE_LIMIT_TRACKER[page_id]["count"] >= MAX_PER_HOUR:
                await asyncio.sleep(60)
                await message_queue.put(job)
                message_queue.task_done()
                continue

            # STEP 1: Wait 5-10 seconds before public reply
            delay_public = random.randint(5, 10)
            print(f"🎯 MATCH FOUND! Waiting {delay_public}s before public reply to {sender_name}...")
            await asyncio.sleep(delay_public)
            
            try:
                reply_text = get_unique_reply()
                reply_url = f"https://graph.facebook.com/v19.0/{comment_id}/comments"
                res_reply = await client.post(
                    reply_url,
                    data={"message": reply_text},
                    params={"access_token": token}
                )
                if res_reply.status_code == 200:
                    print(f"✅ Public reply posted: \"{reply_text}\"")
                else:
                    print(f"❌ META API COMMENT ERROR ({res_reply.status_code}): {res_reply.text}")
            except Exception as e:
                print(f"❌ NETWORK EXCEPTION POSTING REPLY: {e}")

            # STEP 2: Wait another 10-20 seconds before sending DM
            delay_dm = random.randint(10, 20)
            print(f"⏳ Waiting {delay_dm}s before sending DM to {sender_name}...")
            await asyncio.sleep(delay_dm)

            full_name = sender_name.strip() if sender_name else "there"
            first_name = full_name.split(" ")[0] if full_name != "there" else "there"
            
            personalized_text = campaign["dm_text"].replace("{first_name}", first_name).replace("{full_name}", full_name)
            
            if campaign.get("button_url"):
                link_title = campaign.get("button_text", "Click Here") if campaign.get("button_text") else "Click Here"
                tracking_url = f"{base_url}/click/{campaign['id']}"
                
                payload = {
                    "recipient": {"comment_id": comment_id},
                    "message": {
                        "attachment": {
                            "type": "template",
                            "payload": {
                                "template_type": "button",
                                "text": personalized_text,
                                "buttons": [{"type": "web_url", "url": tracking_url, "title": link_title}]
                            }
                        }
                    }
                }
            else:
                payload = {
                    "recipient": {"comment_id": comment_id},
                    "message": {"text": personalized_text}
                }

            url = f"https://graph.facebook.com/v19.0/{page_id}/messages"

            try:
                res = await client.post(url, json=payload, params={"access_token": token})
                if res.status_code == 200:
                    RATE_LIMIT_TRACKER[page_id]["count"] += 1
                    with get_db() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("UPDATE campaigns SET dms_sent = dms_sent + 1 WHERE id = %s", (campaign["id"],))
                        conn.commit()
                    print(f"✅ DM sent successfully to {sender_name}!")
                else:
                    print(f"❌ META API DM ERROR ({res.status_code}): {res.text}")
            except Exception as e:
                print(f"❌ FATAL ERROR SENDING DM: {e}")
            message_queue.task_done()

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(process_queue())

# =========================================================
# 7. META WEBHOOK
# =========================================================
@app.get("/webhook")
async def verify_webhook(request: Request):
    if request.query_params.get("hub.mode") == "subscribe" and request.query_params.get("hub.verify_token") == VERIFY_TOKEN:
        return PlainTextResponse(request.query_params.get("hub.challenge"))
    return PlainTextResponse("Token Mismatch", status_code=403)

@app.post("/webhook")
async def handle_webhook(request: Request):
    try:
        body_bytes = await request.body()
        if not body_bytes:
            return PlainTextResponse("EVENT_RECEIVED", status_code=200)
        data = await request.json()
    except Exception as e:
        return PlainTextResponse("EVENT_RECEIVED", status_code=200)

    try:
        for entry in data.get("entry", []):
            page_id = str(entry.get("id"))
            
            if "messaging" in entry:
                for msg_event in entry.get("messaging", []):
                    if "read" in msg_event:
                        with get_db() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute("UPDATE pages SET dms_opened = dms_opened + 1 WHERE page_id = %s", (page_id,))
                                cursor.execute("UPDATE campaigns SET dms_opened = dms_opened + 1 WHERE page_id = %s AND is_active = 1", (page_id,))
                            conn.commit()
                            
            for change in entry.get("changes", []):
                value = change.get("value", {})
                
                if change.get("field") == "feed" and value.get("verb") == "add":
                    item = value.get("item", "")
                    if item != "comment":
                        continue

                    comment_text = value.get("message", "").strip().lower()
                    comment_id = value.get("comment_id")
                    sender_name = value.get("from", {}).get("name", "there")
                    sender_id = value.get("from", {}).get("id", "")
                    raw_post_id = str(value.get("post_id", ""))
                    post_id = raw_post_id.split("_")[-1] if "_" in raw_post_id else raw_post_id

                    # Prevent bot from self-triggering
                    if sender_id == page_id:
                        continue

                    with get_db() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT access_token FROM pages WHERE page_id = %s", (page_id,))
                            page_row = cursor.fetchone()
                            if not page_row:
                                continue
                            
                            cursor.execute("SELECT * FROM campaigns WHERE page_id = %s AND post_id = %s AND is_active = 1", (page_id, post_id))
                            campaign_row = cursor.fetchone()
                            if not campaign_row:
                                cursor.execute("SELECT * FROM campaigns WHERE page_id = %s AND post_id = 'ALL_POSTS' AND is_active = 1", (page_id,))
                                campaign_row = cursor.fetchone()

                            if campaign_row:
                                keywords = [k.strip().lower() for k in campaign_row["trigger_keywords"].split(",") if k.strip()]
                                is_matched = any(kw in comment_text for kw in keywords) if keywords != ["*"] else True
                                
                                if is_matched:
                                    await message_queue.put({
            
