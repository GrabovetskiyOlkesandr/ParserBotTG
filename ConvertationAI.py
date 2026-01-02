import re

def cleanText(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def cutText(text: str, limit: int) -> str:
    text = cleanText(text)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"

def formatTelegramMessage(row) -> str:
    vacancyId, category, title, company, cities, experience, url, description, createdAt, sentTg = row

    title = cleanText(title) or "Vacancy"
    company = cleanText(company)
    cities = cleanText(cities)
    experience = cleanText(experience)
    category = cleanText(category)
    url = cleanText(url)
    shortDesc = cutText(description, 350)

    metaParts = []
    if company:
        metaParts.append(company)
    if cities:
        metaParts.append(cities)
    if experience:
        metaParts.append(experience)
    if category:
        metaParts.append(category)

    lines = [title]
    if metaParts:
        lines.append(" | ".join(metaParts))
    if shortDesc:
        lines.append("")
        lines.append(shortDesc)
    lines.append("")
    lines.append(url)

    text = "\n".join(lines)
    if len(text) > 3900:
        text = text[:3899] + "…"
    return text
