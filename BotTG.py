import os
import time
import requests

from db import ensureSchema, fetchUnsentForTelegram, markSentTelegram
from ConvertationAI import formatTelegramMessage

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

botToken = os.getenv("BOT_TOKEN", "").strip()
chatId = os.getenv("TELEGRAM_CHAT_ID", "").strip()
delaySeconds = float(os.getenv("TELEGRAM_DELAY_SECONDS", "1.2"))
sendLimit = int(os.getenv("TELEGRAM_LIMIT", "10"))

def sendMessage(text: str) -> None:
    if not botToken or not chatId:
        raise RuntimeError("BOT_TOKEN or TELEGRAM_CHAT_ID is empty")

    url = f"https://api.telegram.org/bot{botToken}/sendMessage"
    payload = {
        "chat_id": chatId,
        "text": text,
        "disable_web_page_preview": False,
    }

    for _ in range(6):
        r = requests.post(url, json=payload, timeout=30)
        if r.status_code == 429:
            try:
                data = r.json()
                waitSec = int(data.get("parameters", {}).get("retry_after", 2))
            except Exception:
                waitSec = 2
            time.sleep(waitSec + 1)
            continue

        r.raise_for_status()
        return

    raise RuntimeError("Telegram rate limit")

def main() -> None:
    ensureSchema()
    rows = fetchUnsentForTelegram(limit=sendLimit)

    if not rows:
        print("Nothing to send")
        return

    print(f"Sending: {len(rows)}")

    sentIds = []
    for row in rows:
        text = formatTelegramMessage(row)
        sendMessage(text)
        sentIds.append(int(row[0]))
        title = row[2] or ""
        print(title)
        time.sleep(delaySeconds)

    markSentTelegram(sentIds)
    print(f"Done: {len(sentIds)}")

if __name__ == "__main__":
    main()
