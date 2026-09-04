import os
import json
import time
import aiohttp
import aiosqlite
from typing import Any
from database import DB_PATH

SERVICE_CONFIG = {
    "piarflow": {
        "base_url": "https://piarflow.com/api/v1",
        "key_env": "PIARFLOW_API_KEY",
    },
}
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
        print(f"API Error: {e}")
        return {}
    return {}

def status_name(status: Any, subscribed: Any = None) -> str:
    if subscribed is True or str(status).lower() in ("subscribed", "active", "success"):
        return "subscribed"
    return "unsubscribed"

def normalize_offers(service: str, payload: Any) -> tuple[list[dict], str | None]:
    tasks = []
    items = payload.get("sponsors") or payload.get("channels") or []
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        link = item.get("link") or item.get("url") or item.get("invite_link")
        if not link:
            continue
        if link.startswith(("t.me/", "telegram.me/")):
            link = f"https://{link}"
        tasks.append({
            "service": service,
            "id": str(item.get("id") or item.get("channel_id") or i + 1),
            "link": link,
            "status": status_name(item.get("status"), item.get("subscribed"))
        })
    return tasks, str(payload.get("status", "")).lower()

async def fetch_sponsors_from_service(service: str, user_id: int) -> list[dict]:
    api_key = os.getenv(SERVICE_CONFIG[service]["key_env"], "").strip()
    if not api_key:
        return []
    if service == "piarflow":
        payload = await http_json(
            "GET",
            f"{SERVICE_CONFIG['piarflow']['base_url']}/sponsors",
            params={"api_key": api_key, "telegram_id": user_id}
        )
        return normalize_offers(service, payload)[0]
    return []

async def check_subscriptions_on_service(service: str, user_id: int) -> bool:
    api_key = os.getenv(SERVICE_CONFIG[service]["key_env"], "").strip()
    if not api_key:
        return True
    if service == "piarflow":
        payload = await http_json(
            "POST",
            f"{SERVICE_CONFIG['piarflow']['base_url']}/check",
            json_body={"api_key": api_key, "telegram_id": user_id}
        )
        items = payload.get("sponsors") or payload.get("channels") or []
        return all(status_name(i.get("status"), i.get("subscribed")) == "subscribed" for i in items if isinstance(i, dict))
    return True

async def get_sponsors_for_user(user_id: int, force_refresh: bool = False) -> list[dict]:
    tasks = []
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cache_key = f"piarflow:{user_id}"
        
        async with db.execute("SELECT payload, expires_at FROM sponsor_cache WHERE cache_key = ?", (cache_key,)) as cursor:
            cached = await cursor.fetchone()
            
        if not force_refresh and cached and float(cached["expires_at"]) > time.time():
            tasks.extend(json.loads(cached["payload"]))
        else:
            new_tasks = await fetch_sponsors_from_service("piarflow", user_id)
            await db.execute(
                "INSERT OR REPLACE INTO sponsor_cache(cache_key, payload, expires_at) VALUES (?,?,?)",
                (cache_key, json.dumps(new_tasks), time.time() + SPONSOR_CACHE_SECONDS)
            )
            await db.commit()
            tasks.extend(new_tasks)
            
    return tasks
