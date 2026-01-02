import os
import sqlite3
import csv
from datetime import datetime
from typing import List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

dbFile = os.getenv("DB_FILE", "vacancies.db")

def connectDb() -> sqlite3.Connection:
    return sqlite3.connect(dbFile)

def ensureSchema() -> None:
    conn = connectDb()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS vacancies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            title TEXT,
            company TEXT,
            cities TEXT,
            experience TEXT,
            url TEXT UNIQUE,
            description TEXT,
            created_at TEXT NOT NULL,
            sent_tg INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    cur.execute("PRAGMA table_info(vacancies);")
    cols = {row[1] for row in cur.fetchall()}

    if "sent_tg" not in cols:
        cur.execute("ALTER TABLE vacancies ADD COLUMN sent_tg INTEGER NOT NULL DEFAULT 0;")

    if "sent_tg_at" not in cols:
        cur.execute("ALTER TABLE vacancies ADD COLUMN sent_tg_at TEXT;")

    cur.execute("CREATE INDEX IF NOT EXISTS idxVacanciesCategory ON vacancies(category);")
    cur.execute("CREATE INDEX IF NOT EXISTS idxVacanciesExperience ON vacancies(experience);")
    cur.execute("CREATE INDEX IF NOT EXISTS idxVacanciesCities ON vacancies(cities);")
    cur.execute("CREATE INDEX IF NOT EXISTS idxVacanciesSent ON vacancies(sent_tg);")

    conn.commit()
    conn.close()

def create_table() -> None:
    ensureSchema()

def insertVacancy(category: str, title: str, company: str, cities: str, experience: str, url: str, description: str) -> bool:
    conn = connectDb()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO vacancies(category, title, company, cities, experience, url, description, created_at, sent_tg)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0);
            """,
            (
                category,
                title,
                company,
                cities,
                experience,
                url,
                description,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def remove_duplicates() -> None:
    conn = connectDb()
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM vacancies
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM vacancies
            GROUP BY url
        );
        """
    )
    conn.commit()
    conn.close()

def fetch_latest(limit: int = 20) -> List[Tuple]:
    conn = connectDb()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, category, title, company, cities, experience, url, description, created_at, sent_tg
        FROM vacancies
        ORDER BY id DESC
        LIMIT ?;
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows

def fetchUnsentForTelegram(limit: int = 10) -> List[Tuple]:
    conn = connectDb()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, category, title, company, cities, experience, url, description, created_at, sent_tg
        FROM vacancies
        WHERE sent_tg = 0
        ORDER BY id ASC
        LIMIT ?;
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows

def markSentTelegram(ids: List[int]) -> None:
    if not ids:
        return
    conn = connectDb()
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    q = ",".join(["?"] * len(ids))
    cur.execute(
        f"""
        UPDATE vacancies
        SET sent_tg = 1, sent_tg_at = ?
        WHERE id IN ({q});
        """,
        [now] + ids,
    )
    conn.commit()
    conn.close()

def search_vacancies(keyword: Optional[str] = None, category: Optional[str] = None, city: Optional[str] = None,
                     experience: Optional[str] = None, limit: int = 50) -> List[Tuple]:
    conn = connectDb()
    cur = conn.cursor()

    where = []
    params = []

    if category:
        where.append("category = ?")
        params.append(category)

    if experience:
        where.append("experience = ?")
        params.append(experience)

    if city:
        where.append("LOWER(cities) LIKE ?")
        params.append(f"%{city.lower()}%")

    if keyword:
        where.append("(LOWER(title) LIKE ? OR LOWER(company) LIKE ? OR LOWER(description) LIKE ?)")
        kw = f"%{keyword.lower()}%"
        params.extend([kw, kw, kw])

    whereSql = (" WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT id, category, title, company, cities, experience, url, description, created_at, sent_tg
        FROM vacancies
        {whereSql}
        ORDER BY id DESC
        LIMIT ?;
    """
    params.append(limit)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows

def stats_by_category() -> List[Tuple[str, int]]:
    conn = connectDb()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT category, COUNT(*) as cnt
        FROM vacancies
        GROUP BY category
        ORDER BY cnt DESC;
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows

def export_to_csv(path: str = "vacancies_export.csv") -> None:
    conn = connectDb()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, category, title, company, cities, experience, url, created_at, sent_tg
        FROM vacancies
        ORDER BY id DESC;
        """
    )
    rows = cur.fetchall()
    conn.close()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "category", "title", "company", "cities", "experience", "url", "created_at", "sent_tg"])
        writer.writerows(rows)
