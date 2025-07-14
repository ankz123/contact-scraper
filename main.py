import os
import re
import csv
import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from datetime import datetime

app = FastAPI()

# ------------------ UTILS ------------------ #

EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
PHONE_REGEX = r"(?:(?:\+91[\-\s]?)|(?:0))?[6-9]\d{9}"

JUNK_EMAIL_SUBSTRINGS = [
    "sentry.io", "sentry.wixpress.com", "wixpress.com",
    "polyfill", "core-js", "react", "lodash", "focus"
]

def is_valid_email(email):
    if re.search(r"@\d", email):
        return False
    return not any(bad in email for bad in JUNK_EMAIL_SUBSTRINGS)

def normalize_phone(number):
    digits = re.sub(r"\D", "", number)
    if len(digits) == 10:
        return f"+91{digits}"
    if len(digits) == 12 and digits.startswith("91"):
        return f"+{digits}"
    return None

async def fetch(session, url):
    try:
        async with session.get(url, timeout=20) as resp:
            if resp.status == 200:
                return await resp.text()
    except Exception:
        return None

async def find_contact_page(session, base_url):
    try:
        html = await fetch(session, base_url)
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"].lower()
            if any(k in href for k in ["contact", "get-in-touch", "reach-us"]):
                full_url = urljoin(base_url, href)
                return full_url
    except Exception:
        return None
    return None

def extract_visible_phones(soup):
    visible_phones = set()
    tags_to_check = ["a", "p", "div", "span", "li", "td"]
    for tag in soup.find_all(tags_to_check):
        text = tag.get_text(separator=" ", strip=True)
        found = re.findall(PHONE_REGEX, text)
        visible_phones.update(found)
    return visible_phones

async def scrape_site(session, url):
    emails = set()
    phones = set()
    error = None

    try:
        contact_url = await find_contact_page(session, url) or url
        html = await fetch(session, contact_url)

        if not html:
            return {
                "url": url,
                "emails": [],
                "phones": [],
                "error": "Site not reachable",
                "contact_page": contact_url
            }

        soup = BeautifulSoup(html, "html.parser")
        emails.update(re.findall(EMAIL_REGEX, html))
        visible_phones = extract_visible_phones(soup)
        phones.update(visible_phones)

        for a in soup.find_all("a", href=True):
            href = a["href"]
            emails.update(re.findall(EMAIL_REGEX, href))
            phones.update(re.findall(PHONE_REGEX, href))

        emails = {e.strip() for e in emails if is_valid_email(e)}
        phones = {normalize_phone(p) for p in phones if normalize_phone(p)}

    except Exception as e:
        error = str(e)

    return {
        "url": url,
        "contact_page": contact_url,
        "emails": list(emails),
        "phones": list(phones),
        "error": error
    }

# ------------------ BULK ------------------ #

async def extract_contacts_bulk(urls: list[str]):
    filename = f"results/results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    os.makedirs("results", exist_ok=True)

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[scrape_site(session, url) for url in urls])

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Contact Page", "Emails", "Phones", "Error"])
        for r in results:
            writer.writerow([
                r["url"],
                r.get("contact_page", ""),
                ", ".join(r["emails"]),
                ", ".join(r["phones"]),
                r["error"] or ""
            ])

    return os.path.basename(filename)

# ------------------ ROUTES ------------------ #

@app.get("/extract")
async def extract_single(url: str = Query(...)):
    async with aiohttp.ClientSession() as session:
        return await scrape_site(session, url)

class BulkInput(BaseModel):
    urls: list[str]

@app.post("/extract/bulk")
async def extract_bulk(data: BulkInput):
    filename = await extract_contacts_bulk(data.urls)
    return {"csv_url": f"/download/{filename}"}

@app.post("/extract/upload")
async def extract_from_file(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        return {"error": "Only CSV files supported."}
    try:
        df = pd.read_csv(file.file, header=None)
        urls = df[0].dropna().tolist()
        filename = await extract_contacts_bulk(urls)
        return {"csv_url": f"/download/{filename}"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/download/{filename}")
async def download_file(filename: str):
    path = os.path.join("results", filename)
    if not os.path.exists(path):
        return JSONResponse(status_code=404, content={"error": "File not found"})
    return FileResponse(path=path, filename=filename, media_type="text/csv")
