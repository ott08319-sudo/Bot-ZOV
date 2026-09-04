import json
import time
import aiohttp
import aiosqlite
from config import PIARFLOW_API_KEY, DB_PATH

BASE_URL = "https://piarflow.com/api/v1"
SPONSOR_CACHE_SECONDS = 300

async def http_json(method: str, url: str, params: dict = None, json_body: dict = None) -> dict:
    try:
        async with aiohttp.ClientSession() as session:
            if method == "GET":
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    return await resp.json()
            elif method == "POST":
                async with session.post(url, json=json_body, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    return await resp.json()
    except Exception as e:
        print(f"PiarFlow API Error: {e}")
        return {}
    return {}

async def fetch_sponsors_from_service(user_id: int) -> list[dict]:
    if not PIARFLOW_API_KEY:
        return []
    payload = await http_json("GET", f"{BASE_URL}/sponsors", params={"api_key": PIARFLOW_API_KEY, "telegram_id": user_id})
    items = payload.get("sponsors") or payload.get("channels") or []
    tasks = []
    for i, item in enumerate(items):
        if isinstance(item, dict):
            link = item.get("link") or item.get("url") or item.get("invite_link")
            if link:
                if link.startswith(("t.me/", "telegram.me/")):
                    link = f"https://{link}"
                tasks.append({"id": str(item.get("id") or i + 1), "link": link})
    return tasks

async def check_subscriptions_on_service(user_id: int) -> bool:
    if not PIARFLOW_API_KEY:
        return True
    payload = await http_json("POST", f"{BASE_URL}/check", json_body={"api_key": PIARFLOW_API_KEY, "telegram_id": user_id})
    items = payload.get("sponsors") or payload.get("channels") or []
    for item in items:
        if isinstance(item, dict):
            status = str(item.get("status")).lower()
            if status not in ("subscribed", "active", "success") and not item.get("subscribed"):
                return False
    return True

async def get_sponsors_for_user(user_id: int, force_refresh: bool = False) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cache_key = f"piarflow:{user_id}"
        async with db.execute("SELECT payload, expires_at FROM sponsor_cache WHERE cache_key = ?", (cache_key,)) as cursor:
            cached = await cursor.fetchone()
            
        if not force_refresh and cached and float(cached["expires_at"]) > time.time():
            return json.loads(cached["payload"])
            
        new_tasks = await fetch_sponsors_from_service(user_id)
        await db.execute(
            "INSERT OR REPLACE INTO sponsor_cache(cache_key, payload, expires_at) VALUES (?,?,?)",
            (cache_key, json.dumps(new_tasks), time.time() + SPONSOR_CACHE_SECONDS)
        )
        await db.commit()
        return new_tasks
