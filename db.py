import sqlite3

DB_FILE = "vacancies.db"

def execute_query(query, params=(), fetch=False):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        if fetch:
            return cur.fetchall()
        conn.commit()

def create_table():
    execute_query("""
    CREATE TABLE IF NOT EXISTS vacancies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT, company TEXT, cities TEXT, job_name TEXT,
        experience TEXT, url TEXT UNIQUE, description TEXT,
        category TEXT, is_sent INTEGER DEFAULT 0
    )""")

def insert_vacancy(title, company, cities, job_name, experience, url, description):
    query = "INSERT OR IGNORE INTO vacancies (title, company, cities, job_name, experience, url, description) VALUES (?, ?, ?, ?, ?, ?, ?)"
    execute_query(query, (title, company, cities, job_name, experience, url, description))

def remove_duplicates():
    query = "DELETE FROM vacancies WHERE id NOT IN (SELECT MIN(id) FROM vacancies GROUP BY url)"
    execute_query(query)