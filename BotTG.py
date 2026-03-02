import sqlite3
import requests
import time
import os
from dotenv import load_dotenv
from ConvertationAI import get_unsent_vacancies_by_category, process_vacancy_row

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

CHANNELS = {
    "Android": os.getenv("CHANNEL_ANDROID"),
    "C++": os.getenv("CHANNEL_CPP"),
    "Data Science": os.getenv("CHANNEL_DATA_SCIENCE"),
    "Java": os.getenv("CHANNEL_JAVA"),
    "iOS/MacOS": os.getenv("CHANNEL_IOS"),
    "DevOps": os.getenv("CHANNEL_DEVOPS"),
    "Front End": os.getenv("CHANNEL_FRONTEND"),
    "HR": os.getenv("CHANNEL_HR"),
    "PHP": os.getenv("CHANNEL_PHP"),
    "Python": os.getenv("CHANNEL_PYTHON"),
    "Ruby": os.getenv("CHANNEL_RUBY"),
    "SEO": os.getenv("CHANNEL_SEO"),
    "Support": os.getenv("CHANNEL_SUPPORT"),
    "Unity": os.getenv("CHANNEL_UNITY"),
    "Unreal Engine": os.getenv("CHANNEL_UNREAL")
}

BATCH_SIZE = 10
PAUSE_BETWEEN_MSGS = 10
PAUSE_BETWEEN_BATCHES = 60


def send_to_telegram(text, chat_id):
    if not BOT_TOKEN:
        print(" ПОМИЛКА: Не знайдено BOT_TOKEN в .env")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    while True:
        try:
            response = requests.post(url, data=payload, timeout=10)
            result = response.json()
            if result.get("ok"):
                return True
            elif result.get("error_code") == 429:
                retry_after = result["parameters"]["retry_after"]
                print(f"Ліміт Telegram API. Чекаємо {retry_after} сек...")
                time.sleep(retry_after + 1)
            elif result.get("error_code") == 400 and "chat not found" in result.get("description", ""):
                print(f"ПОМИЛКА: Бот не доданий в адміни каналу {chat_id}!")
                return False
            else:
                print(f" Помилка Telegram API: {result}")
                return False
        except requests.RequestException as e:
            print(f" Помилка запиту до Telegram: {e}")
            time.sleep(5)


def send_all_categories(db_path="vacancies.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for category_title, chat_id in CHANNELS.items():
        if not chat_id:
            continue

        print(f"\n Починаємо розсилку для {category_title} → {chat_id}")

        while True:
            rows = get_unsent_vacancies_by_category(category_title, db_path)
            if not rows:
                print(f" Всі вакансії для {category_title} надіслані. Переходимо до наступної категорії.")
                break

            batch = rows[:BATCH_SIZE]
            sent_count = 0

            for row in batch:
                vid, msg = process_vacancy_row(row)
                success = send_to_telegram(msg, chat_id)
                if success:
                    cursor.execute("UPDATE vacancies SET is_sent = 1 WHERE id = ?", (vid,))
                    conn.commit()
                    sent_count += 1
                time.sleep(PAUSE_BETWEEN_MSGS)

            print(f" Відправлено {sent_count} вакансій у {category_title}.")
            print(f"⏸ Чекаємо {PAUSE_BETWEEN_BATCHES} секунд перед наступною серією...")
            time.sleep(PAUSE_BETWEEN_BATCHES)

    conn.close()
    print("\n Усі категорії відправлені!")


if __name__ == "__main__":
    send_all_categories()