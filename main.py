from fastapi import FastAPI, Query, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
import asyncio
import re
import aiohttp
import os
import csv
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import uuid
import pandas as pd

app = FastAPI()

# Ensure output dir exists
os.makedirs("results", exist_ok=True)

EMAIL_REGEX = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
PHONE_REGEX = r'(?:\+91[-\s]?|0)?(?:[6-9]\d{9})'

HEADERS = {"User-Agent": "Mozilla/5.0"}

EXCLUDED_EMAILS = ["sentry.wixpress.com", "sentry.io", "wixpress.com", "sentry-next.wixpress.com"]


def normalize_phone(phone):
    digits = re.sub(r'\D', '', phone)
    return '+91' + digits[-10:] if len(digits) >= 10 else None


async def fetch_html(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=15) as resp:
            if resp.status == 200:
                return await resp.text()
    except Exception:
        return None


def extract_contacts(html):
    emails = list(set(re.findall(EMAIL_REGEX, html or "")))
    phones = list(set(re.findall(PHONE_REGEX, html or "")))

    # Clean up
    phones = list({normalize_phone(p) for p in phones if normalize_phone(p)})
    emails = [e for e in emails if not any(x in e for x in EXCLUDED_EMAILS)]
    return emails, phones


async def find_contact_page(session, base_url):
    try:
        html = await fetch_html(session, base_url)
        if not html:
            return base_url
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href'].lower()
            if any(x in href for x in ['contact', 'connect', 'support']):
                contact_url = urljoin(base_url, a['href'])
                return contact_url
    except Exception:
        pass
    return base_url


async def scrape_site(session, url):
    result = {"url": url, "emails": [], "phones": [], "error": None}
    try:
        contact_page = await find_contact_page(session, url)
        html = await fetch_html(session, contact_page)
        if html:
            emails, phones = extract_contacts(html)
            result["emails"] = emails
            result["phones"] = phones
            result["contact_page"] = contact_page
        else:
            result["error"] = "No response from site"
    except Exception as e:
        result["error"] = str(e)
    return result


async def extract_contacts_bulk(urls, concurrency=10):
    connector = aiohttp.TCPConnector(limit_per_host=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [scrape_site(session, url) for url in urls]
        responses = await asyncio.gather(*tasks)

    # Retry failed
    failed = [r["url"] for r in responses if r["error"]]
    if failed:
        async with aiohttp.ClientSession(connector=connector) as session:
            retry_tasks = [scrape_site(session, url) for url in failed]
            retry_responses = await asyncio.gather(*retry_tasks)
        for retry in retry_responses:
            for i, r in enumerate(responses):
                if r["url"] == retry["url"] and r["error"]:
                    responses[i] = retry

    filename = f"{uuid.uuid4().hex[:8]}.csv"
    filepath = os.path.join("results", filename)
    with open(filepath, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["url", "contact_page", "emails", "phones", "error"])
        for r in responses:
            writer.writerow([
                r["url"],
                r.get("contact_page", ""),
                ", ".join(r["emails"]),
                ", ".join(r["phones"]),
                r.get("error", "")
            ])
    return filename


# ------------------ ROUTES ------------------ #

@app.get("/extract")
async def extract_single(url: str = Query(...)):
    return await scrape_site(aiohttp.ClientSession(), url)


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
        filename = await asyncio.to_thread(extract_contacts_bulk, urls)
        return {"csv_url": f"/download/{filename}"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/download/{filename}")
async def download_file(filename: str):
    path = os.path.join("results", filename)
    if not os.path.exists(path):
        return JSONResponse(status_code=404, content={"error": "File not found"})
    return FileResponse(path=path, filename=filename, media_type="text/csv")



