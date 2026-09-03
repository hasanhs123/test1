import os
import sqlite3
import asyncio
import random
import time
import httpx
from typing import Optional
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
import uvicorn

app = FastAPI(title="Comment To DM Engine")

# =========================================================
# 🔴 PASTE YOUR META APP CREDENTIALS HERE
# =========================================================
FB_APP_ID = "4540018746283778"
FB_APP_SECRET = "44b48575462c74e05dc2dae3b7c74886"
VERIFY_TOKEN = "hasan1235"
# =========================================================

DB_FILE = "bot_database.db"
MAX_PER_HOUR = 500
message_queue = asyncio.Queue()
RATE_LIMIT_TRACKER = {}

def get_base_url(request: Request):
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.url.netloc)
    return f"{scheme}://{host}"

# =========================================================
# 1. DATABASE INITIALIZATION
# =========================================================
def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        cursor = conn.cursor()
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_id TEXT NOT NULL,
                post_id TEXT NOT NULL,
                campaign_name TEXT NOT NULL,
                trigger_keywords TEXT NOT NULL,
                dm_text TEXT NOT NULL,
                button_text TEXT NOT NULL,
                button_url TEXT NOT NULL,
                dms_sent INTEGER DEFAULT 0,
                link_clicks INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (page_id) REFERENCES pages (page_id),
                UNIQUE(page_id, post_id)
            )
        """)
        conn.commit()

init_db()

# =========================================================
# 2. FACEBOOK OAUTH LOGIN
# =========================================================
@app.get("/auth/facebook")
async def auth_facebook(request: Request):
    redirect_uri = get_base_url(request) + "/auth/callback"
    scopes = "pages_show_list,pages_read_engagement,pages_manage_posts,pages_messaging,pages_manage_metadata"
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
            return HTMLResponse(f"Failed to get token.")

        pages_url = "https://graph.facebook.com/v19.0/me/accounts"
        pages_res = await client.get(pages_url, params={"access_token": user_token})
        pages_data = pages_res.json().get("data", [])

        with get_db() as conn:
            cursor = conn.cursor()
            for page in pages_data:
                try:
                    await client.post(
                        f"https://graph.facebook.com/v19.0/{page['id']}/subscribed_apps",
                        params={
                            "access_token": page["access_token"],
                            "subscribed_fields": "feed,messages"
                        }
                    )
                except Exception:
                    pass

                cursor.execute("""
                    INSERT OR REPLACE INTO pages (page_id, page_name, access_token) 
                    VALUES (?, ?, ?)
                """, (page["id"], page["name"], page["access_token"]))
            conn.commit()

    return RedirectResponse("/")

# =========================================================
# 3. DYNAMIC POST FETCHER
# =========================================================
@app.get("/api/posts/{page_id}")
async def get_page_posts(page_id: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT access_token FROM pages WHERE page_id = ?", (page_id,))
        row = cursor.fetchone()
        if not row:
            return JSONResponse({"error": "Page not found"}, status_code=404)
        access_token = row["access_token"]
    
    async with httpx.AsyncClient() as client:
        url = f"https://graph.facebook.com/v19.0/{page_id}/published_posts"
        res = await client.get(url, params={"fields": "id,message,created_time,full_picture", "access_token": access_token, "limit": 15})
        return res.json()

# =========================================================
# 4. ASYNC BACKGROUND WORKER (Delay & Messaging)
# =========================================================
async def process_queue():
    async with httpx.AsyncClient(timeout=20.0) as client:
        while True:
            job = await message_queue.get()
            page_id = job["page_id"]
            comment_id = job["comment_id"]
            sender_name = job["sender_name"]
            campaign = job["campaign"]
            token = job["token"]

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

            delay = random.randint(30, 45)
            print(f"🎯 MATCH FOUND! Waiting {delay} seconds before sending DM to {sender_name}...")
            await asyncio.sleep(delay)

            full_name = sender_name.strip() if sender_name else "there"
            first_name = full_name.split(" ")[0] if full_name != "there" else "there"
            personalized_text = campaign["dm_text"].replace("{first_name}", first_name).replace("{full_name}", full_name)

            url = f"https://graph.facebook.com/v19.0/{page_id}/messages"
            payload = {
                "recipient": {"comment_id": comment_id},
                "message": {"text": personalized_text}
            }

            try:
                print(f"🚀 SENDING DM PAYLOAD: {payload}")
                res = await client.post(url, json=payload, params={"access_token": token})
                print(f"📥 META API RESPONSE: {res.status_code} - {res.text}")
                
                if res.status_code == 200:
                    RATE_LIMIT_TRACKER[page_id]["count"] += 1
                    with get_db() as conn:
                        conn.execute("UPDATE campaigns SET dms_sent = dms_sent + 1 WHERE id = ?", (campaign["id"],))
                        conn.commit()
            except Exception as e:
                print(f"❌ FATAL ERROR SENDING DM: {e}")
            message_queue.task_done()

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(process_queue())


# =========================================================
# 5. META WEBHOOK
# =========================================================
@app.get("/webhook")
async def verify_webhook(request: Request):
    if request.query_params.get("hub.mode") == "subscribe" and request.query_params.get("hub.verify_token") == VERIFY_TOKEN:
        return int(request.query_params.get("hub.challenge"))
    return HTMLResponse("Token Mismatch", status_code=403)

@app.post("/webhook")
async def handle_webhook(request: Request):
    data = await request.json()
    print(f"🔔 INCOMING WEBHOOK EVENT: {data}")

    try:
        for entry in data.get("entry", []):
            page_id = str(entry.get("id"))
            if "messaging" in entry:
                for msg_event in entry.get("messaging", []):
                    if "read" in msg_event:
                        with get_db() as conn:
                            conn.execute("UPDATE pages SET dms_opened = dms_opened + 1 WHERE page_id = ?", (page_id,))
                            conn.commit()
            for change in entry.get("changes", []):
                value = change.get("value", {})
                if change.get("field") == "feed" and value.get("item") == "comment" and value.get("verb") == "add":
                    comment_text = value.get("message", "").strip().lower()
                    comment_id = value.get("comment_id")
                    sender_name = value.get("from", {}).get("name", "there")
                    raw_post_id = str(value.get("post_id", ""))
                    post_id = raw_post_id.split("_")[-1] if "_" in raw_post_id else raw_post_id

                    with get_db() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT access_token FROM pages WHERE page_id = ?", (page_id,))
                        page_row = cursor.fetchone()
                        if not page_row:
                            continue
                        
                        cursor.execute("SELECT * FROM campaigns WHERE page_id = ? AND post_id = ? AND is_active = 1", (page_id, post_id))
                        campaign_row = cursor.fetchone()
                        if not campaign_row:
                            cursor.execute("SELECT * FROM campaigns WHERE page_id = ? AND post_id = 'ALL_POSTS' AND is_active = 1", (page_id,))
                            campaign_row = cursor.fetchone()

                        if campaign_row:
                            keywords = [k.strip().lower() for k in campaign_row["trigger_keywords"].split(",") if k.strip()]
                            is_matched = any(kw in comment_text for kw in keywords) if keywords != ["*"] else True
                            
                            if is_matched:
                                await message_queue.put({
                                    "page_id": page_id, "comment_id": comment_id, "sender_name": sender_name,
                                    "token": page_row["access_token"], "campaign": dict(campaign_row)
                                })
    except Exception as e:
        print(f"❌ ERROR PROCESSING WEBHOOK: {e}")
    return {"status": "EVENT_RECEIVED"}

# =========================================================
# 6. DASHBOARD UI (BUTTONS & LINKS REMOVED)
# =========================================================
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pages ORDER BY created_at DESC")
        pages = cursor.fetchall()
        cursor.execute("SELECT c.*, p.page_name FROM campaigns c JOIN pages p ON c.page_id = p.page_id ORDER BY c.id DESC")
        campaigns = cursor.fetchall()

    pages_options = '<option value="">-- Select a Facebook Page --</option>' + "".join([f'<option value="{p["page_id"]}">{p["page_name"]}</option>' for p in pages])
    
    pages_cards_html = ""
    for p in pages:
        tracker = RATE_LIMIT_TRACKER.get(p["page_id"], {"count": 0})
        pages_cards_html += f"""
        <div class="bg-white p-5 rounded-2xl border border-gray-100 shadow-sm flex justify-between items-center">
            <div>
                <h4 class="font-bold text-slate-900 text-sm md:text-base">{p["page_name"]}</h4>
                <p class="text-[11px] text-gray-400 font-mono mt-0.5">ID: {p["page_id"]}</p>
                <span class="inline-block mt-2 bg-blue-50 text-blue-600 font-bold text-[10px] px-2 py-0.5 rounded">Total Opens: {p["dms_opened"]}</span>
            </div>
            <div class="text-right flex items-center gap-4">
                <div>
                    <span class="text-[9px] font-extrabold uppercase tracking-wider text-gray-400 block">Sent / Hour</span>
                    <span class="font-extrabold text-sm text-green-600">{tracker["count"]} / 500</span>
                </div>
            </div>
        </div>
        """

    campaigns_rows_html = ""
    for c in campaigns:
        status = '<span class="bg-green-50 text-green-700 text-[10px] font-extrabold px-2.5 py-1 rounded border border-green-100">ON</span>' if c["is_active"] else '<span class="bg-gray-100 text-gray-500 text-[10px] font-extrabold px-2.5 py-1 rounded">OFF</span>'
        
        actions = f"""
        <div class="flex items-center justify-end gap-3">
            <button onclick="editCampaign({c['id']}, `{c['campaign_name']}`, `{c['trigger_keywords']}`, `{c['dm_text']}`)" class="text-xs font-bold text-blue-500 hover:text-blue-700 transition"><i class="fa-solid fa-pen"></i> Edit</button>
            <form action="/delete-campaign" method="post" onsubmit="return confirm('Delete campaign?');" class="inline m-0 p-0">
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
        <style>.text-brand {{ color: #0fc261; }} .bg-brand {{ background-color: #0fc261; }}</style>
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
                    <a href="/auth/facebook" class="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2.5 rounded-xl text-xs font-bold transition shadow-sm flex items-center gap-2">
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
                                    <th class="py-3 px-4">Campaign</th><th class="py-3 px-4">Target Post</th><th class="py-3 px-4">Keywords</th>
                                    <th class="py-3 px-4 text-center">Sent</th>
                                    <th class="py-3 px-4 text-center">Status</th><th class="py-3 px-4 text-right">Actions</th>
                                </tr>
                            </thead>
                            <tbody>{campaigns_rows_html if campaigns else '<tr><td colspan="6" class="text-center py-8 text-gray-400 text-xs">No active automations.</td></tr>'}</tbody>
                        </table>
                    </div>
                </div>
            </section>
        </main>

        <div id="addCampaignModal" class="hidden fixed inset-0 z-50 bg-slate-900/60 backdrop-blur-sm flex items-center justify-center p-4">
            <div class="bg-white rounded-3xl w-full max-w-lg shadow-2xl p-6 relative max-h-[90vh] flex flex-col">
                <button onclick="document.getElementById('addCampaignModal').classList.add('hidden')" class="absolute top-4 right-4 text-gray-400 hover:text-gray-800"><i class="fa-solid fa-xmark"></i></button>
                <h3 class="text-lg font-extrabold text-slate-900 mb-1">Create Automation Rule</h3>
                <form action="/add-campaign" method="post" class="space-y-4 text-xs overflow-y-auto pr-2 mt-4">
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
                        <div><label class="block font-bold text-gray-600 mb-1">Trigger Words</label><input type="text" name="trigger_keywords" required placeholder="91, 97" class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500"></div>
                    </div>
                    <div>
                        <label class="block font-bold text-gray-600 mb-1">DM Message Text (Use <code>{{{{first_name}}}}</code>)</label>
                        <textarea name="dm_text" required rows="3" placeholder="Hi {{{{first_name}}}}! You got it right!" class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500"></textarea>
                    </div>
                    <button type="submit" class="w-full bg-blue-600 hover:bg-blue-700 text-white py-3 rounded-xl font-bold uppercase tracking-wider text-xs transition mt-2 shadow-md">Deploy Automation</button>
                </form>
            </div>
        </div>

        <div id="editCampaignModal" class="hidden fixed inset-0 z-50 bg-slate-900/60 backdrop-blur-sm flex items-center justify-center p-4">
            <div class="bg-white rounded-3xl w-full max-w-lg shadow-2xl p-6 relative max-h-[90vh] flex flex-col">
                <button onclick="document.getElementById('editCampaignModal').classList.add('hidden')" class="absolute top-4 right-4 text-gray-400 hover:text-gray-800"><i class="fa-solid fa-xmark"></i></button>
                <h3 class="text-lg font-extrabold text-slate-900 mb-1">Edit Automation Rule</h3>
                <form action="/edit-campaign" method="post" class="space-y-4 text-xs overflow-y-auto pr-2 mt-4">
                    <input type="hidden" name="campaign_id" id="edit_campaign_id">
                    <div class="grid grid-cols-2 gap-3">
                        <div><label class="block font-bold text-gray-600 mb-1">Rule Name</label><input type="text" name="campaign_name" id="edit_campaign_name" required class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500"></div>
                        <div><label class="block font-bold text-gray-600 mb-1">Trigger Words</label><input type="text" name="trigger_keywords" id="edit_trigger_keywords" required class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500"></div>
                    </div>
                    <div>
                        <label class="block font-bold text-gray-600 mb-1">DM Message Text</label>
                        <textarea name="dm_text" id="edit_dm_text" required rows="3" class="w-full px-3.5 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:border-blue-500"></textarea>
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

            function editCampaign(id, name, keywords, dm_text) {{
                document.getElementById('edit_campaign_id').value = id;
                document.getElementById('edit_campaign_name').value = name;
                document.getElementById('edit_trigger_keywords').value = keywords;
                document.getElementById('edit_dm_text').value = dm_text;
                document.getElementById('editCampaignModal').classList.remove('hidden');
            }}

            document.getElementById('page_selector').addEventListener('change', async function() {{
                const pageId = this.value;
                const grid = document.getElementById('post_grid');
                const loader = document.getElementById('loading_posts');
                
                if(!pageId) return;
                grid.innerHTML = ''; loader.classList.remove('hidden');
                
                try {{
                    const res = await fetch('/api/posts/' + pageId);
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

@app.post("/add-campaign")
async def add_campaign(page_id: str = Form(...), campaign_name: str = Form(...), post_id: str = Form(...), trigger_keywords: str = Form(...), dm_text: str = Form(...)):
    clean_post_id = post_id.strip().split("_")[-1] if "_" in post_id else post_id.strip()
    with get_db() as conn:
        cursor = conn.cursor()
        # Inserting blank strings for button_text and button_url to prevent SQLite errors
        cursor.execute("INSERT OR REPLACE INTO campaigns (page_id, post_id, campaign_name, trigger_keywords, dm_text, button_text, button_url, is_active) VALUES (?, ?, ?, ?, ?, '', '', 1)", (page_id.strip(), clean_post_id, campaign_name.strip(), trigger_keywords.strip().lower(), dm_text.strip()))
        conn.commit()
    return HTMLResponse("<script>window.location.href='/';</script>")

@app.post("/edit-campaign")
async def edit_campaign(campaign_id: int = Form(...), campaign_name: str = Form(...), trigger_keywords: str = Form(...), dm_text: str = Form(...)):
    with get_db() as conn:
        conn.execute("""
            UPDATE campaigns 
            SET campaign_name = ?, trigger_keywords = ?, dm_text = ?
            WHERE id = ?
        """, (campaign_name.strip(), trigger_keywords.strip().lower(), dm_text.strip(), campaign_id))
        conn.commit()
    return HTMLResponse("<script>window.location.href='/';</script>")

@app.post("/delete-campaign")
async def delete_campaign(campaign_id: int = Form(...)):
    with get_db() as conn:
        conn.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))
        conn.commit()
    return HTMLResponse("<script>window.location.href='/';</script>")

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
