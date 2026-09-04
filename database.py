import os
import aiosqlite
import time

DB_PATH = os.getenv("DB_PATH", "bot.db")

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            balance REAL DEFAULT 0.0,
            total_earned REAL DEFAULT 0.0,
            referrer_id INTEGER,
            referrals_count INTEGER DEFAULT 0,
            tasks_done INTEGER DEFAULT 0,
            last_bonus REAL DEFAULT 0,
            bp_level INTEGER DEFAULT 1,
            bp_xp INTEGER DEFAULT 0,
            created_at REAL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_cache (
            cache_key TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            expires_at REAL NOT NULL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_tasks (
            user_id INTEGER,
            service TEXT,
            offer_id TEXT,
            link TEXT,
            status TEXT DEFAULT 'unsubscribed',
            created_at REAL,
            PRIMARY KEY (user_id, service, offer_id, link)
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            reward REAL,
            uses_left INTEGER
        )""")
        # Создадим тестовый промокод по умолчанию
        await db.execute("INSERT OR IGNORE INTO promo_codes VALUES ('START2026', 10.0, 100)")
        await db.commit()

async def get_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            return dict(row) if row else None

async def register_user(user_id: int, username: str, referrer_id: int = None):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)) as c:
            if await c.fetchone():
                return
        
        now = time.time()
        await db.execute(
            "INSERT INTO users (user_id, username, referrer_id, created_at) VALUES (?, ?, ?, ?)",
            (user_id, username, referrer_id, now)
        )
        
        # Начисление рефереру (3.0 ⭐ за друга)
        if referrer_id and referrer_id != user_id:
            await db.execute("""
                UPDATE users 
                SET balance = balance + 3.0, 
                    total_earned = total_earned + 3.0, 
                    referrals_count = referrals_count + 1 
                WHERE user_id = ?
            """, (referrer_id,))
            
        await db.commit()

async def update_balance(user_id: int, amount: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE users 
            SET balance = balance + ?, 
                total_earned = total_earned + CASE WHEN ? > 0 THEN ? ELSE 0 END 
            WHERE user_id = ?
        """, (amount, amount, amount, user_id))
        await db.commit()
