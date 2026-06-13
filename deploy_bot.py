# ============================================================
# WADDLE DEPLOYER BOT
# Telegram bot that deploys files to GitHub automatically
# Commands:
#   Send a file → deploys to correct repo/path automatically
#   /archive [strategy-name] → next .md file goes to archive repo
#   /status → shows all services status
#   /help → shows available commands
# ============================================================

import os
import httpx
import base64
import json
import time
from datetime import datetime, timezone

DEPLOY_BOT_TOKEN = os.environ.get("DEPLOY_BOT_TOKEN", "")
DEPLOY_CHAT_ID   = os.environ.get("DEPLOY_CHAT_ID", "")
GITHUB_TOKEN     = os.environ.get("GITHUB_TOKEN", "")
GITHUB_USERNAME  = os.environ.get("GITHUB_USERNAME", "mrepsiloned-netizen")

TELEGRAM_API = f"https://api.telegram.org/bot{DEPLOY_BOT_TOKEN}"

# Archive repo
ARCHIVE_REPO = "waddle-strategy-archive"

# File routing — maps filename to repo + path
FILE_ROUTES = {
    "index.html":        {"repo": "fib-backtest-ui",  "path": "index.html"},
    "main.py":           {"repo": "fib-backtest-api", "path": "main.py"},
    "paper_trader.py":   {"repo": "fib-backtest-api", "path": "paper_trader.py"},
    "live_trader.py":    {"repo": "fib-backtest-api", "path": "live_trader.py"},
    "deploy_bot.py":     {"repo": "fib-backtest-api", "path": "deploy_bot.py"},
    "requirements.txt":  {"repo": "fib-backtest-api", "path": "requirements.txt"},
    "Procfile":          {"repo": "fib-backtest-api", "path": "Procfile"},
    "matrix_runner.py":  {"repo": "fib-backtest-api", "path": "matrix_runner.py"},
    "mise.toml":          {"repo": "fib-backtest-api", "path": "mise.toml"},
    "runtime.txt":        {"repo": "fib-backtest-api", "path": "runtime.txt"},
}

# ── ARCHIVE STATE ─────────────────────────────────────────
# Stores pending archive target: {"strategy": "bos-pullback", "set_at": timestamp}
_archive_pending = {}

def set_archive_pending(strategy_name):
    _archive_pending["strategy"] = strategy_name
    _archive_pending["set_at"] = time.time()

def get_archive_pending():
    # Expires after 5 minutes
    if not _archive_pending.get("strategy"):
        return None
    if time.time() - _archive_pending.get("set_at", 0) > 300:
        _archive_pending.clear()
        return None
    return _archive_pending["strategy"]

def clear_archive_pending():
    _archive_pending.clear()

# ── TELEGRAM HELPERS ──────────────────────────────────────
def send(text, parse_mode="HTML"):
    try:
        httpx.post(f"{TELEGRAM_API}/sendMessage", json={
            "chat_id": DEPLOY_CHAT_ID,
            "text": text,
            "parse_mode": parse_mode
        }, timeout=10)
    except Exception as e:
        print(f"Send error: {e}")

def get_updates(offset=0):
    try:
        res = httpx.get(f"{TELEGRAM_API}/getUpdates", params={
            "offset": offset,
            "timeout": 30,
            "allowed_updates": ["message"]
        }, timeout=35)
        if res.status_code == 200:
            return res.json().get("result", [])
    except Exception as e:
        print(f"getUpdates error: {e}")
    return []

def download_file(file_id):
    try:
        res = httpx.get(f"{TELEGRAM_API}/getFile", params={"file_id": file_id}, timeout=10)
        file_path = res.json()["result"]["file_path"]
        file_res  = httpx.get(f"https://api.telegram.org/file/bot{DEPLOY_BOT_TOKEN}/{file_path}", timeout=30)
        return file_res.content
    except Exception as e:
        print(f"Download error: {e}")
        return None

# ── GITHUB HELPERS ────────────────────────────────────────
def get_file_sha(repo, path):
    try:
        url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{repo}/contents/{path}"
        res = httpx.get(url, headers={
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }, timeout=10)
        if res.status_code == 200:
            return res.json().get("sha")
    except Exception as e:
        print(f"Get SHA error: {e}")
    return None

def push_to_github(repo, path, content_bytes, commit_msg):
    try:
        url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{repo}/contents/{path}"
        content_b64 = base64.b64encode(content_bytes).decode("utf-8")
        sha = get_file_sha(repo, path)
        payload = {
            "message": commit_msg,
            "content": content_b64,
            "branch":  "main"
        }
        if sha:
            payload["sha"] = sha
        res = httpx.put(url, json=payload, headers={
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }, timeout=15)
        return res.status_code in [200, 201], res.json()
    except Exception as e:
        return False, {"message": str(e)}

def get_repo_info(repo):
    try:
        url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{repo}"
        res = httpx.get(url, headers={
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }, timeout=10)
        if res.status_code == 200:
            return res.json()
    except:
        pass
    return None

# ── COMMAND HANDLERS ──────────────────────────────────────
def handle_help():
    send("""🤖 <b>Waddle Deployer</b>

<b>How to deploy:</b>
Just send me any of these files and I'll push to GitHub automatically:

📄 <code>index.html</code> → fib-backtest-ui
📄 <code>main.py</code> → fib-backtest-api
📄 <code>paper_trader.py</code> → fib-backtest-api
📄 <code>live_trader.py</code> → fib-backtest-api
📄 <code>deploy_bot.py</code> → fib-backtest-api
📄 <code>requirements.txt</code> → fib-backtest-api
📄 <code>Procfile</code> → fib-backtest-api
📄 <code>matrix_runner.py</code> → fib-backtest-api
📄 <code>mise.toml</code> → fib-backtest-api
📄 <code>runtime.txt</code> → fib-backtest-api

<b>Archive a strategy doc:</b>
1. Send <code>/archive [strategy-name]</code>
   e.g. <code>/archive bos-pullback</code>
2. Then send your .md file
→ Pushes to waddle-strategy-archive/strategies/[name]/

<b>Commands:</b>
/help — show this message
/status — check GitHub repos
/files — list deployable files
/archive [name] — set archive target, then send .md file

Railway auto-deploys within 2 minutes of every push.""")

def handle_status():
    send("⏳ Checking status...")
    msgs = []
    for repo in ["fib-backtest-api", "fib-backtest-ui", ARCHIVE_REPO]:
        info = get_repo_info(repo)
        if info:
            updated = info.get("updated_at","?")[:10]
            msgs.append(f"✅ <b>{repo}</b>\nLast updated: {updated}")
        else:
            msgs.append(f"❌ <b>{repo}</b> — could not reach")
    send("\n\n".join(msgs))

def handle_files():
    lines = ["📁 <b>Deployable files:</b>\n"]
    for filename, route in FILE_ROUTES.items():
        lines.append(f"• <code>{filename}</code> → {route['repo']}")
    lines.append(f"\n📚 <b>Archive:</b> /archive [strategy-name] then send .md")
    send("\n".join(lines))

def handle_archive_command(text):
    parts = text.strip().split(None, 1)
    if len(parts) < 2 or not parts[1].strip():
        send("❓ Usage: <code>/archive [strategy-name]</code>\nExample: <code>/archive bos-pullback</code>")
        return
    strategy = parts[1].strip().lower().replace(" ", "-")
    set_archive_pending(strategy)
    send(f"""📚 <b>Archive mode active</b>

Strategy: <code>{strategy}</code>
Now send your .md file → it will be pushed to:
<code>waddle-strategy-archive/strategies/{strategy}/</code>

⏳ Waiting for file (expires in 5 minutes)""")

def handle_file(message):
    doc = message.get("document")
    if not doc:
        return

    filename = doc.get("file_name", "")
    # Telegram sometimes appends .txt to non-standard files (e.g. mise.toml.txt)
    if filename.endswith(".toml.txt"): filename = filename[:-4]
    if filename.endswith(".txt") and filename not in FILE_ROUTES: filename = filename[:-4]
    file_id  = doc.get("file_id")

    # ── ARCHIVE ROUTE ──────────────────────────────────────
    pending_strategy = get_archive_pending()
    if pending_strategy and filename.endswith(".md"):
        clear_archive_pending()
        archive_path = f"strategies/{pending_strategy}/{filename}"
        send(f"📥 Received <code>{filename}</code>\n⏳ Archiving to <b>{ARCHIVE_REPO}</b>/{archive_path}...")

        content = download_file(file_id)
        if not content:
            send(f"❌ Failed to download {filename}")
            return

        now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        commit_msg = f"Archive {pending_strategy}/{filename} via Telegram — {now_str}"
        success, result = push_to_github(ARCHIVE_REPO, archive_path, content, commit_msg)

        if success:
            commit_url = result.get("commit", {}).get("html_url", "")
            send(f"""✅ <b>Archived successfully!</b>

📄 File: <code>{filename}</code>
📚 Strategy: <b>{pending_strategy}</b>
📦 Repo: <b>{ARCHIVE_REPO}</b>
⏰ {now_str}

{f'🔗 <a href="{commit_url}">View commit</a>' if commit_url else ''}""")
        else:
            error = result.get("message", "Unknown error")
            send(f"❌ Archive failed: {error}")
        return

    # ── STANDARD DEPLOY ROUTE ──────────────────────────────
    if filename not in FILE_ROUTES:
        # Give a hint if they forgot to set archive mode
        if filename.endswith(".md"):
            send(f"❓ To archive a .md file, first send:\n<code>/archive [strategy-name]</code>\nthen resend your file.")
        else:
            send(f"❓ Unknown file: <code>{filename}</code>\n\nSend /files to see deployable files.")
        return

    route = FILE_ROUTES[filename]
    repo  = route["repo"]
    path  = route["path"]

    send(f"📥 Received <code>{filename}</code>\n⏳ Deploying to <b>{repo}</b>...")

    content = download_file(file_id)
    if not content:
        send(f"❌ Failed to download {filename}")
        return

    now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    commit_msg = f"Deploy {filename} via Telegram — {now_str}"
    success, result = push_to_github(repo, path, content, commit_msg)

    if success:
        commit_url = result.get("commit", {}).get("html_url", "")
        send(f"""✅ <b>Deployed successfully!</b>

📄 File: <code>{filename}</code>
📦 Repo: <b>{repo}</b>
⏰ {now_str}

🚀 Railway will redeploy in ~2 minutes
{f'🔗 <a href="{commit_url}">View commit</a>' if commit_url else ''}""")
    else:
        error = result.get("message", "Unknown error")
        send(f"❌ Deploy failed: {error}")

# ── MAIN LOOP ─────────────────────────────────────────────
def run():
    print("🚀 Waddle Deployer starting...")
    send("""🚀 <b>Waddle Deployer is LIVE</b>

Send me any file to deploy it automatically to GitHub.
Use /archive [strategy-name] to archive strategy docs.

Send /help for instructions.""")

    offset = 0
    while True:
        try:
            updates = get_updates(offset)
            for update in updates:
                offset = update["update_id"] + 1
                msg = update.get("message", {})

                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != str(DEPLOY_CHAT_ID):
                    print(f"Unauthorized: {chat_id}")
                    continue

                text = msg.get("text", "")
                if text == "/help":
                    handle_help()
                elif text == "/status":
                    handle_status()
                elif text == "/files":
                    handle_files()
                elif text.startswith("/archive"):
                    handle_archive_command(text)
                elif msg.get("document"):
                    handle_file(msg)
                elif text and not text.startswith("/"):
                    send("💬 Send me a file to deploy, or /help for instructions.")

            time.sleep(1)

        except KeyboardInterrupt:
            print("Bot stopped")
            break
        except Exception as e:
            print(f"Main loop error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run()
