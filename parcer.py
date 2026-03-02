import requests
from bs4 import BeautifulSoup
from db import create_table, insert_vacancy, remove_duplicates
from config import CATEGORIES, EXPERIENCES, HEADERS

create_table()
session = requests.Session()
session.headers.update(HEADERS)


def get_full_description(url: str) -> str:
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        desc_block = soup.select_one("div.vacancy-section")
        return desc_block.get_text(separator="\n", strip=True) if desc_block else ""
    except Exception as e:
        print(f"⚠️ Помилка опису {url}: {e}")
    return ""


def parse_vacancies():
    for category_name, category_url in CATEGORIES.items():
        for exp_key, exp_label in EXPERIENCES.items():
            base_url = f"https://jobs.dou.ua/vacancies/?category={category_url}&exp={exp_key}"
            xhr_url = f"https://jobs.dou.ua/vacancies/xhr-load/?category={category_url}&exp={exp_key}"

            session.get(base_url)  
            csrf_token = session.cookies.get("csrftoken")

            count = 0
            while True:
                payload = {"from": str(count), "count": str(count), "csrfmiddlewaretoken": csrf_token}
                response = session.post(xhr_url, data=payload)

                try:
                    data = response.json()
                except:
                    break

                html_content = data.get("html")
                if not html_content:
                    break

                soup = BeautifulSoup(html_content, "lxml")
                for v in soup.select("li.l-vacancy"):
                    company = v.select_one("a.company").text.strip()
                    cities = v.select_one("span.cities").text.strip() if v.select_one("span.cities") else "—"
                    job_name = v.select_one("a.vt").text.strip()
                    url = v.select_one("a.vt")["href"]

                    description = get_full_description(url)
                    insert_vacancy(category_name, company, cities, job_name, exp_label, url, description)

                if data.get("last"):
                    break
                count += 20


if __name__ == "__main__":
    parse_vacancies()
    remove_duplicates()
    print("Готово.")