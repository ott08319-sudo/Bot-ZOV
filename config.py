import os

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "").strip()
DB_PATH = os.getenv("DB_PATH", "bot.db")
PORT = int(os.getenv("PORT", 10000))

# Экономика бота
REF_REWARD = 3.0           # За приглашенного реферала
DAILY_BONUS = 1.0         # Ежедневный бонус
MIN_WITHDRAW = 15.0       # Минимальный вывод в Stars
TASK_REWARD = 0.25        # Награда за выполнение локального задания
