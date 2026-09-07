import os
import asyncio
import random
import time
import httpx
import psycopg2
import psycopg2.extras
from typing import Optional
from fastapi import FastAPI, Request, Form, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, PlainTextResponse
import uvicorn

app = FastAPI(title="Comment To DM Engine")

# =========================================================
# 🔴 META APP CREDENTIALS (SECURED)
# =========================================================
FB_APP_ID = os.environ.get("FB_APP_ID")
FB_APP_SECRET = os.environ.get("FB_APP_SECRET")
VERIFY_TOKEN = "hasan1235"
# =========================================================

# =========================================================
# 🔒 SECRET URL DASHBOARD SECURITY
# =========================================================
SECRET_ADMIN_PATH = "/earnflow-admin-7788"
# =========================================================

DB_URL = os.environ.get("DATABASE_URL")
message_queue = asyncio.Queue()

# =========================================================
# 📝 RANDOMIZED UNIQUE COMMENT ENGINE
# =========================================================
CORRECT_REPLIES_MASTER = [
    "Correct! Tag a friend to see if they know it.", "Spot on! Share this puzzle to test others.",
    "You nailed it! Hit follow for daily challenges.", "Perfect answer! Challenge your smart friends.",
    "Exactly! See if your friends can solve this.", "Right answer! Share to trick someone else.",
    "100% correct! Tag a genius buddy.", "Brilliant! Follow for more brain busters.",
    "Nailed it! Pass this challenge to a friend.", "Spot on logic! Share if you love trivia.",
    "Yes! Challenge someone to match your speed.", "Right on the money! Follow for daily puzzles.",
    "You got it! Tag a friend who loves riddles.", "Absolute perfection! Share to test your group.",
    "Correct answer! Hit follow to play tomorrow.", "Bingo! Tag someone to test their brain.",
    "Genius level! Share this with your smart friends.", "That is it! Follow us for more.",
    "Exactly right! Tag a friend to compete.", "Perfect! Share to see who else gets it.",
    "Right on! Follow for the next puzzle.", "You solved it! Challenge a coworker.",
    "Correct! Share if you love a good challenge.", "Spot on! Hit follow to keep playing.",
    "Nailed it! Tag someone who needs a brain workout.", "Yes! Share to stump the internet.",
    "100% right! Follow us for daily trivia.", "Brilliant answer! Tag a trivia master.",
    "Exactly! Share to challenge your family.", "Right! Follow so you don't miss tomorrow.",
    "You got it right! Tag a puzzle lover.", "Perfect deduction! Share this post.",
    "Correct! Hit follow for more brain training.", "Spot on answer! Challenge a friend.",
    "Nailed the logic! Share to test others.", "Yes! Tag someone who can match you.",
    "100% accurate! Follow for daily Q&A.", "Brilliant logic! Share with your group chat.",
    "Exactly! Tag a buddy to try.", "Right on! Hit follow to play every day.",
    "You solved it perfectly! Share this puzzle.", "Correct! Challenge your friends now.",
    "Spot on! Follow us for more riddles.", "Nailed it! Tag a smart friend.",
    "Yes, that's the one! Share this brain teaser.", "100% correct! Hit follow.",
    "Brilliant! Tag someone to compete with.", "Exactly right! Share if you had fun.",
    "Right answer! Follow for more.", "You got it! Test your friends next."
]

WRONG_REPLIES_MASTER = [
    "Not quite! Tag a friend to help you out.", "Incorrect! Share this to get some hints.",
    "Close, but no! Challenge a friend to try.", "Oops, that's wrong! Follow for more practice.",
    "Not the answer! Tag a smart buddy for help.", "Try again! Share to see what others think.",
    "Missed the mark! Test your friends instead.", "Not exactly! Hit follow and try tomorrow.",
    "That's incorrect! Ask a friend for the answer.", "Almost! Share this puzzle to get a clue.",
    "Wrong answer! Tag someone who might know.", "Nope! Follow us for daily brain training.",
    "Not it! Share to see if anyone else gets it.", "Incorrect! Challenge a coworker to solve it.",
    "Close but incorrect! Hit follow to keep trying.", "Oops! Tag a friend to crack this.",
    "Not right! Share this challenge.", "Wrong! Follow for more brain busters.",
    "Not quite it! Ask your smart friends.", "Incorrect guess! Share for help.",
    "Nope! Tag a puzzle lover to assist.", "Wrong answer! Hit follow and try again later.",
    "Not the right one! Share to test your family.", "Incorrect! Challenge someone else.",
    "Missed it! Follow us for daily trivia.", "Not exactly! Tag a friend to solve it.",
    "Wrong! Share to stump the internet.", "Nope, try again! Hit follow.",
    "Incorrect! Ask your group chat.", "Not quite! Share if you love a challenge."
]

AVAILABLE_CORRECT = []
AVAILABLE_WRONG = []

def get_unique_reply(is_correct: bool):
    global AVAILABLE_CORRECT, AVAILABLE_WRONG
    if is_correct:
        if not AVAILABLE_CORRECT:
            AVAILABLE_CORRECT = CORRECT_REPLIES_MASTER.copy()
            random.shuffle(AVAILABLE_CORRECT)
        return AVAILABLE_CORRECT.pop()
    else:
        if not AVAILABLE_WRONG:
            AVAILABLE_WRONG = WRONG_REPLIES_MASTER.copy()
            random.shuffle(AVAILABLE_WRONG)
        return AVAILABLE_WRONG.pop()
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
                CREATE TABLE IF NOT EXISTS dm_tracking (
                    user_id TEXT,
                    page_id TEXT,
                    campaign_id INTEGER,
                    is_opened BOOLEAN DEFAULT FALSE,
                    sender_name TEXT DEFAULT '',
                    PRIMARY KEY (user_id, page_id)
                )
            """)
            # New table specifically to log unique link clicks
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS click_tracking (
                    campaign_id INTEGER,
                    user_id TEXT,
                    PRIMARY KEY (campaign_id, user_id)
                )
            """)
            cursor.execute("ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS first_dm_text TEXT DEFAULT ''")
            cursor.execute("ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS dm_trigger_keywords TEXT DEFAULT ''")
            cursor.execute("ALTER TABLE dm_tracking ADD COLUMN IF NOT EXISTS is_opened BOOLEAN DEFAULT FALSE")
            cursor.execute("ALTER TABLE dm_tracking ADD COLUMN IF NOT EXISTS sender_name TEXT DEFAULT ''")
            cursor.execute("ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS dms_opened INTEGER DEFAULT 0")
        conn.commit()

init_db()

# =========================================================
# 2. PUBLIC BLANK PAGE
# =========================================================
@app.get("/")
async def root_blank():
    return PlainTextResponse("Not Found", status_code=404)

@app.get("/ping")
async def keep_alive():
    return PlainTextResponse("Bot is awake and running 24/7!", status_code=200)

# =========================================================
# 3. FACEBOOK OAUTH LOGIN
# =========================================================
@app.get(f"{SECRET_ADMIN_PATH}/auth/facebook")
async def auth_facebook(request: Request):
    redirect_uri = get_base_url(request) + "/auth/callback"
    scopes = "pages_show_list,pages_read_engagement,pages_manage_engagement,pages_manage_posts,pages_messaging,pages_manage_metadata"
    auth_url = f"https://www.facebook.com/v21.0/dialog/oauth?client_id={FB_APP_ID}&redirect_uri={redirect_uri}&scope={scopes}"
    return RedirectResponse(auth_url)

@app.get("/auth/callback")
async def auth_callback(request: Request, code: str = None):
    if not code:
        return HTMLResponse("Authorization failed or denied by user.")

    redirect_uri = get_base_url(request) + "/auth/callback"
    
    async with httpx.AsyncClient() as client:
        token_url = "https://graph.facebook.com/v21.0/oauth/access_token"
        res = await client.get(token_url, params={
            "client_id": FB_APP_ID,
            "redirect_uri": redirect_uri,
            "client_secret": FB_APP_SECRET,
            "code": code
        })
        user_token = res.json().get("access_token")
        
        if not user_token:
            return HTMLResponse(f"Failed to get token: {res.text}")

        pages_url = "https://graph.facebook.com/v21.0/me/accounts"
        pages_res = await client.get(pages_url, params={"access_token": user_token})
        pages_data = pages_res.json().get("data", [])

        with get_db() as conn:
            with conn.cursor() as cursor:
                for page in pages_data:
                    try:
                        sub_res = await client.post(
                            f"https://graph.facebook.com/v21.0/{page['id']}/subscribed_apps",
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

    return RedirectResponse(SECRET_ADMIN_PATH)

# =========================================================
# 4. LINK CLICK TRACKER (UPDATED FOR UNIQUE CLICKS)
# =========================================================
@app.get("/click/{campaign_id}")
async def track_link_click(campaign_id: int, uid: Optional[str] = None):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT button_url FROM campaigns WHERE id = %s", (campaign_id,))
            row = cursor.fetchone()
            
            if row and row["button_url"]:
                if uid:
                    # Check if this user has clicked this specific campaign's link before
                    cursor.execute("SELECT 1 FROM click_tracking WHERE campaign_id = %s AND user_id = %s", (campaign_id, uid))
                    already_clicked = cursor.fetchone()
                    
                    if not already_clicked:
                        # Log them as a new clicker and increase the CTR count by 1
                        cursor.execute("INSERT INTO click_tracking (campaign_id, user_id) VALUES (%s, %s)", (campaign_id, uid))
                        cursor.execute("UPDATE campaigns SET link_clicks = link_clicks + 1 WHERE id = %s", (campaign_id,))
                        conn.commit()
                
                # Always redirect them to the actual URL, even if they've clicked before
                return RedirectResponse(row["button_url"])
            
    return PlainTextResponse("Link expired or invalid.")

# =========================================================
# 5. DYNAMIC POST FETCHER
# =========================================================
@app.get(f"{SECRET_ADMIN_PATH}/api/posts/{{page_id}}")
async def get_page_posts(page_id: str):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT access_token FROM pages WHERE page_id = %s", (page_id,))
            row = cursor.fetchone()
            if not row:
                return JSONResponse({"error": "Page not found"}, status_code=404)
            access_token = row["access_token"]
    
    async with httpx.AsyncClient() as client:
        url = f"https://graph.facebook.com/v21.0/{page_id}/published_posts"
        res = await client.get(url, params={"fields": "id,message,created_time,full_picture", "access_token": access_token, "limit": 15})
        return res.json()

# =========================================================
# 6. ASYNC BACKGROUND WORKER (TWO-STEP FUNNEL ENGINE)
# =========================================================
async def process_queue():
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            job = await message_queue.get()
            job_type = job.get("job_type")
            page_id = job["page_id"]
            sender_id = job["sender_id"]
            sender_name = job["sender_name"]
            campaign = job["campaign"]
            token = job["token"]
            base_url = job["base_url"]

            full_name = sender_name.strip() if sender_name else "there"
            first_name = full_name.split(" ")[0] if full_name != "there" else "there"

            if job_type == "comment_reply":
                comment_id = job["comment_id"]
                is_correct = job["is_correct"]

                delay_public = random.randint(5, 10)
                print(f"🎯 COMMENT DETECTED! Waiting {delay_public}s before public reply to {sender_name}...")
                await asyncio.sleep(delay_public)
                
                try:
                    reply_text = get_unique_reply(is_correct)
                    reply_url = f"https://graph.facebook.com/v21.0/{comment_id}/comments"
                    res_reply = await client.post(reply_url, data={"message": reply_text}, params={"access_token": token})
                    if res_reply.status_code == 200:
                        status_type = "CORRECT" if is_correct else "WRONG"
                        print(f"✅ {status_type} Public reply posted: \"{reply_text}\"")
                except Exception as e:
                    print(f"❌ NETWORK EXCEPTION POSTING REPLY: {e}")

                if not is_correct:
                    print(f"⏭️ Answer incorrect. Skipping DM for {sender_name}.")
                    message_queue.task_done()
                    continue

                delay_dm = random.randint(10, 20)
                print(f"⏳ Waiting {delay_dm}s before sending INITIAL TEXT DM to {sender_name}...")
                await asyncio.sleep(delay_dm)

                raw_first_dm = campaign.get("first_dm_text") or ""
                if not raw_first_dm.strip():
                    print(f"⚠️ WARNING: Initial DM is empty! Using default fallback text.")
                    raw_first_dm = "Hi {first_name}! You got it right! Are you ready for your reward? Reply YES to claim it."

                first_dm_text = raw_first_dm.replace("{first_name}", first_name).replace("{full_name}", full_name)
                invisible_space = "\u200B" * random.randint(1, 5)
                first_dm_text = first_dm_text + invisible_space

                payload = {
                    "recipient": {"comment_id": comment_id},
                    "message": {"text": first_dm_text}
                }
                url = f"https://graph.facebook.com/v21.0/{page_id}/messages"

                try:
                    res = await client.post(url, json=payload, params={"access_token": token})
                    if res.status_code == 200:
                        with get_db() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute("UPDATE campaigns SET dms_sent = dms_sent + 1 WHERE id = %s", (campaign["id"],))
                                cursor.execute("""
                                    INSERT INTO dm_tracking (user_id, page_id, campaign_id, is_opened, sender_name) 
                                    VALUES (%s, %s, %s, FALSE, %s)
                                    ON CONFLICT (user_id, page_id) DO UPDATE SET 
                                    campaign_id = EXCLUDED.campaign_id,
                                    is_opened = FALSE,
                                    sender_name = EXCLUDED.sender_name
                                """, (sender_id, page_id, campaign["id"], sender_name))
                            conn.commit()
                        print(f"✅ INITIAL DM sent to {sender_name}!")
                    else:
                        print(f"❌ META API INITIAL DM ERROR ({res.status_code}): {res.text}")
                except Exception as e:
                    print(f"❌ ERROR SENDING INITIAL DM: {e}")

            elif job_type == "second_dm":
                delay_second_dm = random.randint(5, 8)
                print(f"🎯 TRIGGER WORD MATCHED! Waiting {delay_second_dm}s before sending BUTTON DM to {sender_name}...")
                await asyncio.sleep(delay_second_dm)

                raw_dm_text = campaign.get("dm_text") or ""
                personalized_text = raw_dm_text.replace("{first_name}", first_name).replace("{full_name}", full_name)
                invisible_space = "\u200B" * random.randint(1, 5)
                personalized_text = personalized_text + invisible_space

                if campaign.get("button_url"):
                    # UPDATED: Adding the user's specific sender_id to the tracking URL
                    tracking_url = f"{base_url}/click/{campaign['id']}?uid={sender_id}"
                    link_title = campaign.get("button_text", "Click Here") if campaign.get("button_text") else "Click Here"
                    
                    payload = {
                        "recipient": {"id": sender_id},
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
                        "recipient": {"id": sender_id},
                        "message": {"text": personalized_text}
                    }

                url = f"https://graph.facebook.com/v21.0/{page_id}/messages"

                try:
                    res = await client.post(url, json=payload, params={"access_token": token})
                    if res.status_code == 200:
                        with get_db() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute("DELETE FROM dm_tracking WHERE user_id = %s AND page_id = %s", (sender_id, page_id))
                            conn.commit()
                        print(f"✅ SECOND DM (Payload) sent successfully to {sender_name}!")
                    else:
                        print(f"❌ META API SECOND DM ERROR: {res.text}")
                except Exception as e:
                    print(f"❌ ERROR SENDING SECOND DM: {e}")

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
                        reader_id = msg_event.get("sender", {}).get("id")
                        if reader_id:
                            with get_db() as conn:
                                with conn.cursor() as cursor:
                                    cursor.execute("SELECT campaign_id, is_opened FROM dm_tracking WHERE user_id = %s AND page_id = %s", (reader_id, page_id))
                                    tracking_row = cursor.fetchone()
                                    if tracking_row and not tracking_row["is_opened"]:
                                        cursor.execute("UPDATE pages SET dms_opened = dms_opened + 1 WHERE page_id = %s", (page_id,))
                                        cursor.execute("UPDATE campaigns SET dms_opened = dms_opened + 1 WHERE id = %s", (tracking_row["campaign_id"],))
                                        cursor.execute("UPDATE dm_tracking SET is_opened = TRUE WHERE user_id = %s AND page_id = %s", (reader_id, page_id))
                                conn.commit()
                                
                    elif "message" in msg_event and not msg_event.get("message", {}).get("is_echo"):
                        sender_id = msg_event.get("sender", {}).get("id")
                        message_text = msg_event.get("message", {}).get("text", "").lower()
                        
                        if sender_id and message_text:
                            with get_db() as conn:
                                with conn.cursor() as cursor:
                                    cursor.execute("SELECT campaign_id, sender_name FROM dm_tracking WHERE user_id = %s AND page_id = %s", (sender_id, page_id))
                                    tracking_row = cursor.fetchone()
                                    if tracking_row:
                                        cursor.execute("SELECT * FROM campaigns WHERE id = %s AND is_active = 1", (tracking_row["campaign_id"],))
                                        campaign = cursor.fetchone()
                                        if campaign:
                                            raw_dm_trigger = campaign.get("dm_trigger_keywords") or ""
                                            keywords = [k.strip().lower() for k in raw_dm_trigger.split(",") if k.strip()]
                                            
                                            is_matched = False
                                            if not keywords:
                                                is_matched = False
                                            elif keywords == ["*"]:
                                                is_matched = True
                                            else:
                                                is_matched = any(kw in message_text for kw in keywords)
                                            
                                            if is_matched:
                                                cursor.execute("SELECT access_token FROM pages WHERE page_id = %s", (page_id,))
                                                page_row = cursor.fetchone()
                                                if page_row:
                                                    await message_queue.put({
                                                        "job_type": "second_dm",
                                                        "page_id": page_id,
                                                        "sender_id": sender_id,
                                                        "sender_name": tracking_row["sender_name"],
                                                        "token": page_row["access_token"],
                                                        "campaign": dict(campaign),
                                                        "base_url": get_base_url(request)
                                                    })
                            
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
                                raw_trigger_kw = campaign_row.get("trigger_keywords") or ""
                                keywords = [k.strip().lower() for k in raw_trigger_kw.split(",") if k.strip()]
                                
                                is_matched = False
                                if not keywords:
                                    is_matched = False
                                elif keywords == ["*"]:
                                    is_matched = True
                                else:
                                    is_matched = any(kw in comment_text for kw in keywords)
                                
                                await message_queue.put({
                                    "job_type": "comment_reply",
                                    "page_id": page_id,
                                    "comment_id": comment_id,
                                    "sender_name": sender_name,
                                    "sender_id": sender_id,
                                    "token": page_row["access_token"],
                                    "campaign": dict(campaign_row),
                                    "base_url": get_base_url(request),
                                    "is_correct": is_matched
                                })
    except Exception as e:
        print(f"❌ ERROR PROCESSING WEBHOOK LOGIC: {e}")
        
    return PlainTextResponse("EVENT_RECEIVED", status_code=200)

# =========================================================
# 8. DASHBOARD UI
# =========================================================
@app.get(SECRET_ADMIN_PATH, response_class=HTMLResponse)
async def dashboard():
    def escape_val(v):
        if not v:
            return ""
        return str(v).replace("\\", "\\\\").replace("'", "\\'").replace('"', '&quot;').replace("\n", "\\n").replace("\r", "")

    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM pages ORDER BY created_at DESC")
            pages = cursor.fetchall()
            cursor.execute("SELECT c.*, p.page_name FROM campaigns c JOIN pages p ON c.page_id = p.page_id ORDER BY c.id DESC")
            campaigns = cursor.fetchall()

    pages_options = '<option value="">-- Select a Facebook Page --</option>' + "".join([f'<option value="{p["page_id"]}">{p["page_name"]}</option>' for p in pages])
    
    pages_cards_html = ""
    for p in pages:
        pages_cards_html += f"""
        <div class="bg-white p-5 rounded-2xl border border-gray-100 shadow-sm flex justify-between items-center">
            <div>
                <h4 class="font-bold text-slate-900 text-sm md:text-base">{p["page_name"]}</h4>
                <p class="text-[11px] text-gray-400 font-mono mt-0.5">ID: {p["page_id"]}</p>
                <span class="inline-block mt-2 bg-blue-50 text-blue-600 font-bold text-[10px] px-2 py-0.5 rounded">Total Opens: {p["dms_opened"]}</span>
            </div>
            <div class="text-right flex items-center gap-4">
                <div>
                    <span class="text-[9px] font-extrabold uppercase tracking-wider text-gray-400 block">Status</span>
                    <span class="font-extrabold text-sm text-green-600">Active</span>
                </div>
            </div>
        </div>
        """

    campaigns_rows_html = ""
    for c in campaigns:
        status = '<span class="bg-green-50 text-green-700 text-[10px] font-extrabold px-2.5 py-1 rounded border border-green-100">ON</span>' if c["is_active"] else '<span class="bg-gray-100 text-gray-500 text-[10px] font-extrabold px-2.5 py-1 rounded">OFF</span>'
        ctr = round((c["link_clicks"] / c["dms_sent"]) * 100, 1) if c["dms_sent"] > 0 else 0
        
        safe_name = escape_val(c.get('campaign_name'))
        safe_kw = escape_val(c.get('trigger_keywords'))
        safe_first_dm = escape_val(c.get('first_dm_text'))
        safe_dm_trigger = escape_val(c.get('dm_trigger_keywords'))
        safe_dm = escape_val(c.get('dm_text'))
        safe_btn_txt = escape_val(c.get('button_text'))
        safe_btn_url = escape_val(c.get('button_url'))

        actions = f"""
        <div class="flex items-center justify-end gap-3">
            <button onclick="editCampaign({c['id']}, '{safe_name}', '{safe_kw}', '{safe_first_dm}', '{safe_dm_trigger}', '{safe_dm}', '{safe_btn_txt}', '{safe_btn_url}')" class="text-xs font-bold text-blue-500 hover:text-blue-700 transition"><i class="fa-solid fa-pen"></i> Edit</button>
            <form action="{SECRET_ADMIN_PATH}/delete-campaign" method="post" onsubmit="return confirm('Delete campaign?');" class="inline m-0 p-0">
                <input type="hidden" name="campaign_id" value="{c['id']}">
                <button type="submit" class="text-xs font-bold text-red-400 hover:text-red-600 transition"><i class="fa-solid fa-trash"></i></button>
            </form>
        </div>
        """
        
        campaigns_rows_html += f"""
        <tr class="border-b border-gray-50 hover:bg-gray-50/50 text-xs">
            <td class="py-4 px-4 font-bold text-slate-900">{c["campaign_name"]}<br><span class="text-[10px] text-gray-400 font-normal">Page: {c["page_name"]}</span></td>
            <td class="py-4 px-4 font-mono font-bold text-blue-600 truncate max-w-[100px]" title="{c["post_id"]}">{c["post_id"]}</td>
            <td class="py-4 px-4"><span class="bg-gray-100 text-slate-700 px-2 py-1 rounded font-mono text-[11px] break-all">{c["trigger_keywords"]}</span></td>
            <td class="py-4 px-4 text-center font-bold text-slate-800">{c["dms_sent"]}</td>
            <td class="py-4 px-4 text-center font-bold text-blue-600">{c["dms_opened"]}</td>
            <td class="py-4 px-4 text-center font-bold text-purple-600">{c["link_clicks"]}</td>
            <td class="py-4 px-4 text-center font-bold text-emerald-600">{ctr}%</td>
            <td class="py-4 px-4 text-center">{status}</td>
            <td class="py-4 px-4 text-right">{actions}</td>
        </tr>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>EarnFlow Bot Control Panel</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    </head>
    <body class="bg-[#f8fafc] text-slate-800 antialiased font-sans pb-16">
        <header class="bg-white border-b border-gray-100 sticky top-0 z-30 shadow-sm">
            <div class="max-w-7xl mx-auto px-6 h-20 flex justify-between items-center">
                <div class="flex items-center gap-3">
                    <div class="w-10 h-10 bg-blue-50 text-blue-600 rounded-xl flex items-center justify-center font-bold text-xl"><i class="fa-solid fa-robot"></i></div>
                    <div><h1 class="text-lg font-extrabold text-slate-900 leading-tight">EarnFlow Auto-DM Core</h1><p class="text-[11px] text-gray-400 font-semibold uppercase tracking-wider">Manychat-Style Automation</p></div>
                </div>
            </div>
        </header>

        <main class="max-w-7xl mx-auto px-6 py-10 space-y-10">
            <section class="space-y-4">
                <div class="flex justify-between items-center">
                    <h2 class="text-base font-extrabold text-slate-900 uppercase tracking-wider">1. Connected Pages</h2>
                    <a href="{SECRET_ADMIN_PATH}/auth/facebook" class="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2.5 rounded-xl text-xs font-bold transition shadow-sm flex items-center gap-2">
                        <i class="fa-brands fa-facebook-f"></i> Login with Facebook
                    </a>
                </div>
                <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                    {pages_cards_html if pages else '<div class="col-span-3 text-center py-6 text-gray-400 text-xs font-medium bg-white rounded-2xl border border-dashed border-gray-200">No pages. Click Login with Facebook to connect your accounts automatically.</div>'}
                </div>
            </section>

            <section class="space-y-4">
                <div class="flex justify-between items-center">
                    <h2 class="text-base font-extrabold text-slate-900 uppercase tracking-wider">2. Automation Rules</h2>
                    <button onclick="document.getElementById('addCampaignModal').classList.remove('hidden')" class="bg-slate-900 hover:bg-slate-800 text-white px-4 py-2.5 rounded-xl text-xs font-bold transition flex items-center gap-2"><i class="fa-solid fa-plus"></i> New Automation</button>
                </div>
                <div class="bg-white rounded-3xl border border-gray-100 shadow-sm overflow-hidden">
                    <div class="overflow-x-auto">
                        <table class="w-full text-left border-collapse">
                            <thead>
                                <tr class="bg-gray-50/70 border-b border-gray-100 text-[10px] font-extrabold text-gray-400 uppercase tracking-wider">
                                    <th class="py-3 px-4">Campaign</th>
                                    <th class="py-3 px-4">Target Post</th>
                                    <th class="py-3 px-4">Keywords</th>
                                    <th class="py-3 px-4 text-center">Sent</th>
                                    <th class="py-3 px-4 text-center">Opened</th>
                                    <th class="py-3 px-4 text-center">Clicks</th>
                                    <th class="py-3 px-4 text-center">CTR</th>
                                    <th class="py-3 px-4 text-center">Status</th>
                                    <th class="py-3 px-4 text-right">Actions</th>
                                </tr>
                            </thead>
                            <tbody>{campaigns_rows_html if campaigns else '<tr><td colspan="9" class="text-center py-8 text-gray-400 text-xs">No active automations.</td></tr>'}</tbody>
                        </table>
                    </div>
                </div>
            </section>
        </main>

        <!-- ADD MODAL -->
        <div id="addCampaignModal" class="hidden fixed inset-0 z-50 bg-slate-900/60 backdrop-blur-sm flex items-center justify-center p-4">
            <div class="bg-white rounded-3xl w-full max-w-lg shadow-2xl p-6 relative max-h-[90vh] flex flex-col">
                <button onclick="document.getElementById('addCampaignModal').classList.add('hidden')" class="absolute top-4 right-4 text-gray-400 hover:text-gray-800"><i class="fa-solid fa-xmark"></i></button>
                <h3 class="text-lg font-extrabold text-slate-900 mb-1">Create Automation Rule</h3>
                <form action="{SECRET_ADMIN_PATH}/add-campaign" method="post" class="space-y-4 text-xs overflow-y-auto pr-2 mt-4">
                    <div>
                        <label class="block font-bold text-gray-600 mb-1">Target Page</label>
                        <select name="page_id" id="page_selector" required class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500">
                            {pages_options}
                        </select>
                    </div>
                    <div>
                        <label class="block font-bold text-gray-600 mb-1 flex justify-between"><span>Select Target Video/Post</span> <span id="loading_posts" class="hidden text-blue-600"><i class="fa-solid fa-spinner fa-spin"></i> Fetching...</span></label>
                        <div id="post_grid" class="grid grid-cols-1 gap-2 max-h-40 overflow-y-auto border border-gray-200 p-2 rounded-xl bg-gray-50 mb-2">
                            <div class="text-gray-400 text-center py-4">Select a Page above to load your recent posts.</div>
                        </div>
                        <input type="hidden" name="post_id" id="hidden_post_id" required>
                        <button type="button" onclick="selectAllPosts()" class="w-full py-2 bg-slate-100 text-slate-600 rounded-lg font-bold hover:bg-slate-200 transition">Or apply to ALL future posts on this page</button>
                    </div>
                    <div class="grid grid-cols-2 gap-3">
                        <div><label class="block font-bold text-gray-600 mb-1">Rule Name</label><input type="text" name="campaign_name" required placeholder="e.g. Puzzle Rule" class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500"></div>
                        <div><label class="block font-bold text-gray-600 mb-1">Comment Trigger (Use * for all)</label><input type="text" name="trigger_keywords" required placeholder="91, 97" class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500"></div>
                    </div>
                    
                    <div class="p-3 bg-blue-50 border border-blue-100 rounded-xl space-y-3">
                        <div>
                            <label class="block font-bold text-blue-800 mb-1">Step 1: Initial DM Text (Plain Text)</label>
                            <textarea name="first_dm_text" required rows="2" placeholder="Hi {{{{first_name}}}}! Do you want to get $5000/month ebook guide?" class="w-full px-3.5 py-2.5 bg-white border border-blue-200 rounded-xl focus:outline-none focus:border-blue-500"></textarea>
                        </div>
                        <div>
                            <label class="block font-bold text-blue-800 mb-1">Step 2: User Reply Trigger Words</label>
                            <input type="text" name="dm_trigger_keywords" required placeholder="yes, sure, send it" class="w-full px-3.5 py-2.5 bg-white border border-blue-200 rounded-xl focus:outline-none focus:border-blue-500">
                        </div>
                    </div>

                    <div>
                        <label class="block font-bold text-gray-600 mb-1">Step 3: Final DM Text (With Button)</label>
                        <textarea name="dm_text" required rows="2" placeholder="Here is the link as promised!" class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500"></textarea>
                    </div>
                    <div class="grid grid-cols-2 gap-3 bg-gray-50 p-3 rounded-xl border border-gray-200">
                        <div><label class="block font-bold text-gray-600 mb-1">Link Title (Optional)</label><input type="text" name="button_text" placeholder="e.g. Download Now" class="w-full px-3.5 py-2 bg-white border border-gray-200 rounded-lg focus:outline-none focus:border-blue-500"></div>
                        <div><label class="block font-bold text-gray-600 mb-1">URL Link (Optional)</label><input type="text" name="button_url" placeholder="https://..." class="w-full px-3.5 py-2 bg-white border border-gray-200 rounded-lg focus:outline-none focus:border-blue-500"></div>
                    </div>
                    <button type="submit" class="w-full bg-blue-600 hover:bg-blue-700 text-white py-3 rounded-xl font-bold uppercase tracking-wider text-xs transition mt-2 shadow-md">Deploy Automation</button>
                </form>
            </div>
        </div>

        <!-- EDIT MODAL -->
        <div id="editCampaignModal" class="hidden fixed inset-0 z-50 bg-slate-900/60 backdrop-blur-sm flex items-center justify-center p-4">
            <div class="bg-white rounded-3xl w-full max-w-lg shadow-2xl p-6 relative max-h-[90vh] flex flex-col">
                <button onclick="document.getElementById('editCampaignModal').classList.add('hidden')" class="absolute top-4 right-4 text-gray-400 hover:text-gray-800"><i class="fa-solid fa-xmark"></i></button>
                <h3 class="text-lg font-extrabold text-slate-900 mb-1">Edit Automation Rule</h3>
                <form action="{SECRET_ADMIN_PATH}/edit-campaign" method="post" class="space-y-4 text-xs overflow-y-auto pr-2 mt-4">
                    <input type="hidden" name="campaign_id" id="edit_campaign_id">
                    <div class="grid grid-cols-2 gap-3">
                        <div><label class="block font-bold text-gray-600 mb-1">Rule Name</label><input type="text" name="campaign_name" id="edit_campaign_name" required class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500"></div>
                        <div><label class="block font-bold text-gray-600 mb-1">Comment Trigger (Use * for all)</label><input type="text" name="trigger_keywords" id="edit_trigger_keywords" required class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500"></div>
                    </div>

                    <div class="p-3 bg-blue-50 border border-blue-100 rounded-xl space-y-3">
                        <div>
                            <label class="block font-bold text-blue-800 mb-1">Step 1: Initial DM Text (Plain Text)</label>
                            <textarea name="first_dm_text" id="edit_first_dm_text" required rows="2" class="w-full px-3.5 py-2.5 bg-white border border-blue-200 rounded-xl focus:outline-none focus:border-blue-500"></textarea>
                        </div>
                        <div>
                            <label class="block font-bold text-blue-800 mb-1">Step 2: User Reply Trigger Words</label>
                            <input type="text" name="dm_trigger_keywords" id="edit_dm_trigger_keywords" required class="w-full px-3.5 py-2.5 bg-white border border-blue-200 rounded-xl focus:outline-none focus:border-blue-500">
                        </div>
                    </div>

                    <div>
                        <label class="block font-bold text-gray-600 mb-1">Step 3: Final DM Text (With Button)</label>
                        <textarea name="dm_text" id="edit_dm_text" required rows="2" class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500"></textarea>
                    </div>
                    <div class="grid grid-cols-2 gap-3 bg-gray-50 p-3 rounded-xl border border-gray-200">
                        <div><label class="block font-bold text-gray-600 mb-1">Link Title (Optional)</label><input type="text" name="button_text" id="edit_button_text" class="w-full px-3.5 py-2 bg-white border border-gray-200 rounded-lg focus:outline-none focus:border-blue-500"></div>
                        <div><label class="block font-bold text-gray-600 mb-1">URL Link (Optional)</label><input type="text" name="button_url" id="edit_button_url" class="w-full px-3.5 py-2 bg-white border border-gray-200 rounded-lg focus:outline-none focus:border-blue-500"></div>
                    </div>
                    <button type="submit" class="w-full bg-green-600 hover:bg-green-700 text-white py-3 rounded-xl font-bold uppercase tracking-wider text-xs transition mt-2 shadow-md">Save Changes</button>
                </form>
            </div>
        </div>

        <script>
            function selectAllPosts() {{
                document.getElementById('hidden_post_id').value = 'ALL_POSTS';
                document.querySelectorAll('.post-card').forEach(c => c.classList.remove('ring-2', 'ring-blue-500', 'bg-blue-50'));
                alert("Rule will trigger on EVERY post for this page.");
            }}

            function selectPost(postId, element) {{
                document.getElementById('hidden_post_id').value = postId;
                document.querySelectorAll('.post-card').forEach(c => c.classList.remove('ring-2', 'ring-blue-500', 'bg-blue-50'));
                element.classList.add('ring-2', 'ring-blue-500', 'bg-blue-50');
            }}

            function editCampaign(id, name, keywords, first_dm, dm_trigger, dm_text, btn_text, btn_url) {{
                document.getElementById('edit_campaign_id').value = id;
                document.getElementById('edit_campaign_name').value = name;
                document.getElementById('edit_trigger_keywords').value = keywords;
                document.getElementById('edit_first_dm_text').value = first_dm;
                document.getElementById('edit_dm_trigger_keywords').value = dm_trigger;
                document.getElementById('edit_dm_text').value = dm_text;
                document.getElementById('edit_button_text').value = btn_text;
                document.getElementById('edit_button_url').value = btn_url;
                document.getElementById('editCampaignModal').classList.remove('hidden');
            }}

            document.getElementById('page_selector').addEventListener('change', async function() {{
                const pageId = this.value;
                const grid = document.getElementById('post_grid');
                const loader = document.getElementById('loading_posts');
                
                if(!pageId) return;
                grid.innerHTML = ''; loader.classList.remove('hidden');
                
                try {{
                    const res = await fetch('{SECRET_ADMIN_PATH}/api/posts/' + pageId);
                    const data = await res.json();
                    loader.classList.add('hidden');
                    
                    if(data.data && data.data.length > 0) {{
                        data.data.forEach(post => {{
                            const msg = post.message ? post.message.substring(0, 50) + '...' : 'Media Post';
                            const date = new Date(post.created_time).toLocaleDateString();
                            const cleanId = post.id.includes('_') ? post.id.split('_')[1] : post.id;
                            const imgHtml = post.full_picture ? `<img src="${{post.full_picture}}" class="w-10 h-10 object-cover rounded shadow-sm mr-3 shrink-0">` : `<div class="w-10 h-10 bg-gray-200 rounded mr-3 shrink-0 flex items-center justify-center text-gray-400"><i class="fa-solid fa-image"></i></div>`;
                            
                            grid.innerHTML += `
                                <div onclick="selectPost('${{cleanId}}', this)" class="post-card cursor-pointer bg-white p-2 border border-gray-200 rounded-lg flex items-center hover:bg-blue-50 transition">
                                    ${{imgHtml}}
                                    <div class="overflow-hidden">
                                        <p class="text-xs font-bold text-slate-800 truncate">${{msg}}</p>
                                        <p class="text-[10px] text-gray-400 font-mono mt-0.5">${{date}}</p>
                                    </div>
                                </div>`;
                        }});
                    }} else {{
                        grid.innerHTML = '<div class="text-gray-400 text-xs text-center py-4">No recent posts found.</div>';
                    }}
                }} catch (e) {{
                    loader.classList.add('hidden');
                    grid.innerHTML = '<div class="text-red-400 text-xs text-center py-4">Error loading posts. Make sure permissions are correct.</div>';
                }}
            }});
        </script>
    </body>
    </html>
    """
    return HTMLResponse(html)

@app.post(f"{SECRET_ADMIN_PATH}/add-campaign")
async def add_campaign(
    page_id: str = Form(...), 
    campaign_name: str = Form(...), 
    post_id: str = Form(...), 
    trigger_keywords: str = Form(...), 
    first_dm_text: str = Form(...),
    dm_trigger_keywords: str = Form(...),
    dm_text: str = Form(...), 
    button_text: str = Form(""), 
    button_url: str = Form("")
):
    clean_post_id = post_id.strip().split("_")[-1] if "_" in post_id else post_id.strip()
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO campaigns (page_id, post_id, campaign_name, trigger_keywords, first_dm_text, dm_trigger_keywords, dm_text, button_text, button_url, is_active) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                ON CONFLICT (page_id, post_id) DO UPDATE SET 
                campaign_name = EXCLUDED.campaign_name,
                trigger_keywords = EXCLUDED.trigger_keywords,
                first_dm_text = EXCLUDED.first_dm_text,
                dm_trigger_keywords = EXCLUDED.dm_trigger_keywords,
                dm_text = EXCLUDED.dm_text,
                button_text = EXCLUDED.button_text,
                button_url = EXCLUDED.button_url,
                is_active = 1
            """, (page_id.strip(), clean_post_id, campaign_name.strip(), trigger_keywords.strip().lower(), first_dm_text.strip(), dm_trigger_keywords.strip().lower(), dm_text.strip(), button_text.strip(), button_url.strip()))
        conn.commit()
    return RedirectResponse(url=SECRET_ADMIN_PATH, status_code=303)

@app.post(f"{SECRET_ADMIN_PATH}/edit-campaign")
async def edit_campaign(
    campaign_id: int = Form(...), 
    campaign_name: str = Form(...), 
    trigger_keywords: str = Form(...), 
    first_dm_text: str = Form(...),
    dm_trigger_keywords: str = Form(...),
    dm_text: str = Form(...), 
    button_text: str = Form(""), 
    button_url: str = Form("")
):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE campaigns 
                SET campaign_name = %s, trigger_keywords = %s, first_dm_text = %s, dm_trigger_keywords = %s, dm_text = %s, button_text = %s, button_url = %s
                WHERE id = %s
            """, (campaign_name.strip(), trigger_keywords.strip().lower(), first_dm_text.strip(), dm_trigger_keywords.strip().lower(), dm_text.strip(), button_text.strip(), button_url.strip(), campaign_id))
        conn.commit()
    return RedirectResponse(url=SECRET_ADMIN_PATH, status_code=303)

@app.post(f"{SECRET_ADMIN_PATH}/delete-campaign")
async def delete_campaign(
    campaign_id: int = Form(...)
):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM campaigns WHERE id = %s", (campaign_id,))
        conn.commit()
    return RedirectResponse(url=SECRET_ADMIN_PATH, status_code=303)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
