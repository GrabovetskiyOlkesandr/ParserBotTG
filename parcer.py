import os
import time
import argparse
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from db import ensureSchema, insertVacancy, remove_duplicates

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

timeoutSeconds = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
delaySeconds = float(os.getenv("REQUEST_DELAY_SECONDS", "0.9"))
maxDescChars = int(os.getenv("MAX_DESCRIPTION_CHARS", "20000"))
userAgent = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

baseUrl = "https://jobs.dou.ua"
listUrl = f"{baseUrl}/vacancies/"

categories = {
    "Android": "android",
    "C++": "c++",
    "Data Science": "data-science",
    "Java": "java",
    "iOS/MacOS": "ios",
    "DevOps": "devops",
    "Front End": "front-end",
    "HR": "hr",
    "Python": "python",
    "QA": "qa",
    "Project Manager": "pm",
    "Product Manager": "product-manager",
    "Design": "design",
}

experienceMap = {
    "Без досвіду": "0-1",
    "1–3 роки": "1-3",
    "3–5 років": "3-5",
    "5+ років": "5plus",
}

def makeSession() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": userAgent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": listUrl,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    r = s.get(listUrl, timeout=timeoutSeconds)
    r.raise_for_status()
    return s

def fetchListPage(s: requests.Session, categoryCode: str, page: int, expCode: Optional[str]) -> str:
    params = {"category": categoryCode, "page": page}
    if expCode:
        params["exp"] = expCode
    r = s.get(listUrl, params=params, timeout=timeoutSeconds)
    r.raise_for_status()
    return r.text

def parseCards(html: str) -> List[Tuple[str, str, str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    result: List[Tuple[str, str, str, str]] = []

    for item in soup.select(".vacancy"):
        link = item.select_one("a.vt")
        if not link or not link.get("href"):
            continue

        title = link.get_text(strip=True)
        url = urljoin(baseUrl, link["href"].strip())

        companyEl = item.select_one(".company")
        company = companyEl.get_text(strip=True) if companyEl else ""

        citiesEl = item.select_one(".cities")
        cities = citiesEl.get_text(" ", strip=True) if citiesEl else ""

        result.append((title, company, cities, url))

    if not result:
        for link in soup.select("a.vt[href]"):
            title = link.get_text(strip=True)
            url = urljoin(baseUrl, link["href"].strip())
            wrapper = link.find_parent()
            companyEl = wrapper.select_one(".company") if wrapper else None
            citiesEl = wrapper.select_one(".cities") if wrapper else None
            company = companyEl.get_text(strip=True) if companyEl else ""
            cities = citiesEl.get_text(" ", strip=True) if citiesEl else ""
            if title and url:
                result.append((title, company, cities, url))

    return result

def getDescription(s: requests.Session, url: str) -> str:
    r = s.get(url, timeout=timeoutSeconds)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    blocks = [
        soup.select_one(".b-typo.vacancy-section"),
        soup.select_one(".b-typo"),
        soup.select_one(".l-vacancy"),
        soup.select_one(".vacancy-section"),
    ]

    text = ""
    for b in blocks:
        if b:
            text = b.get_text("\n", strip=True)
            if text:
                break

    text = (text or "").strip()
    if len(text) > maxDescChars:
        text = text[:maxDescChars].rstrip() + "…"
    return text

def runParse(categoryNames: List[str], experienceLabel: Optional[str], maxPages: int) -> None:
    ensureSchema()

    expCode = None
    if experienceLabel:
        expCode = experienceMap.get(experienceLabel)
        if not expCode:
            raise ValueError("Unknown experience value")

    s = makeSession()

    seen = 0
    added = 0

    for name in categoryNames:
        if name not in categories:
            raise ValueError("Unknown category")

        code = categories[name]
        print(f"Category: {name}")

        page = 1
        donePages = 0

        while True:
            if maxPages > 0 and donePages >= maxPages:
                break

            html = fetchListPage(s, code, page, expCode)
            cards = parseCards(html)

            if not cards:
                break

            for title, company, cities, url in cards:
                seen += 1
                time.sleep(delaySeconds)

                try:
                    desc = getDescription(s, url)
                except Exception:
                    desc = ""

                ok = insertVacancy(
                    category=name,
                    title=title,
                    company=company,
                    cities=cities,
                    experience=experienceLabel or "",
                    url=url,
                    description=desc,
                )

                if ok:
                    added += 1
                    print(f"Added: {title} | {company}")
                else:
                    print(f"Duplicate: {title}")

            page += 1
            donePages += 1

    remove_duplicates()
    print(f"Done. Seen: {seen}. Added: {added}.")

def buildArgs() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="parcer.py")
    p.add_argument("--categories", nargs="*", default=["Android"])
    p.add_argument("--experience", type=str, default=None)
    p.add_argument("--max-pages", type=int, default=2)
    return p

def main() -> None:
    args = buildArgs().parse_args()
    runParse(args.categories, args.experience, args.max_pages)

if __name__ == "__main__":
    main()
