import os
import re
import sqlite3
from dotenv import load_dotenv
import google.generativeai as genai
import time

load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    raise ValueError("Не знайдено GEMINI_API_KEY. Перевірте ваш .env файл.")

genai.configure(api_key=API_KEY)


def process_description(description: str) -> str:
    time.sleep(5)

    if not description or not description.strip():
        return "Опис відсутній."

    prompt = f"""
Перетвори цей текст вакансії у стислий формат для Telegram, використовуючи HTML-теги.
Не додавай жодних код-блоків або ```html```.
Формат має бути готовий для надсилання у Telegram з parse_mode='HTML'.

1. Якщо текст англійською — переклади українською.
2. Скороти до 5–7 ключових речень.
3. Видали Markdown, зірочки, лапки та зайві символи.
4. Структуруй у три частини:

   <b>📝 Вступ:</b> кого шукають і з яким досвідом
   <b>💻 Вимоги:</b> основні навички та технології
   <b>🎁 Умови:</b> що компанія пропонує
5. Пиши просто, зрозуміло, з емодзі, без води.

Текст вакансії:
{description}
"""

    try:
        model = genai.GenerativeModel("gemini-flash-latest")
        response = model.generate_content(prompt)
        result = response.text.strip() if response.text else "Не вдалося обробити опис через фільтри."

        result = re.sub(r"```[a-zA-Z]*", "", result)
        result = result.replace("```", "").strip()

        result = result.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")

        return result

    except Exception as e:
        print(f" Помилка при зверненні до Gemini API: {e}")
        return "Не вдалося обробити опис вакансії."


def get_unsent_vacancies_by_category(title: str, db_path="vacancies.db", limit=10):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, title, company, cities, experience, url, description
        FROM vacancies
        WHERE title = ? AND is_sent = 0
        LIMIT ?
    """, (title, limit))
    rows = cursor.fetchall()
    conn.close()
    return rows


def process_vacancy_row(row):
    vid, title, company, cities, experience, url, description = row
    short_desc = process_description(description)
    exp_cleaned = re.sub(r'[^0-9\-+]', '', experience)
    exp_tag = f"#{exp_cleaned}" if exp_cleaned else ""
    msg = (
        f"<b>{title}</b> {exp_tag}\n\n"
        f"🏢 <b>Компанія:</b> {company}\n"
        f"📍 <b>Місто:</b> {cities}\n"
        f"🕒 <b>Досвід:</b> {experience}\n\n"
        f"{short_desc}\n\n"
        f"🔗 <b>Деталі за посиланням:</b>\n{url}"
    )
    return vid, msg


if __name__ == "__main__":
    vacancies = get_unsent_vacancies_by_category("Android", limit=1)
    for vac in vacancies:
        vid, msg = process_vacancy_row(vac)
        print(msg)