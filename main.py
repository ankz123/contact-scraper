import os
import re
import csv
import aiohttp
import asyncio
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

app = FastAPI()

EMAIL_REGEX = re.compile(r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}")
# This pattern captures global numbers including +91, +1, +44 etc. and local formats
PHONE_REGEX = re.compile(r"(?:\+?\d{1,4}[-.\s]?)?(?:\(?\d{2,5}\)?[-.\s]?)?\d{3,4}[-.\s]?\d{3,4}")
JUNK_EMAIL_DOMAINS = {"sentry.wixpress.com", "sentry.io", "sentry-next.wixpress.com"}

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ----------- HTML Fetch Logic with HTTPS → HTTP Fallback ------------ #

async def fetch_html(session, url):
    try:
        async with session.get(url, timeout=10, allow_redirects=True) as response:
            if response.status == 200:
                return await response.text(), str(response.url)
    except:
        pass
    return None, url

async def try_https_then_http(session, raw_url):
    parsed_url = raw_url.strip().replace("http://", "").replace("https://", "")
    for scheme in ["https://", "http://"]:
        html, final_url = await fetch_html(session, scheme + parsed_url)
        if html:
            return html, final_url
    return None, raw_url

# ----------- Extraction Logic ------------ #

def extract_contacts(html):
    emails = set()
    phones = set()

    if not html:
        return [], []

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()

    for match in EMAIL_REGEX.findall(text):
        domain = match.split("@")[-1]
        if domain not in JUNK_EMAIL_DOMAINS:
            emails.add(match.strip())

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.startswith("mailto:"):
            email = href[7:].split("?")[0]
            domain = email.split("@")[-1]
            if domain not in JUNK_EMAIL_DOMAINS:
                emails.add(email.strip())
        elif href.startswith("tel:"):
            phone = re.sub(r"[^\d+]", "", href[4:])
            phones.add(phone)

    for match in PHONE_REGEX.findall(text):
        phone = re.sub(r"[^\d+]", "", match)
        if len(phone) >= 7:
            phones.add(phone)

    return list(emails), list(phones)

async def find_contact_page(session, base_url, html):
    if not html:
        return base_url

    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        href = link["href"].lower()
        if any(kw in href for kw in ["contact", "contact-us"]):
            return urljoin(base_url, href)
    return base_url

# ----------- Scraper Per Site ------------ #

async def scrape_site(session, url):
    result = {"url": url, "emails": [], "phones": [], "error": None, "contact_page": None}

    try:
        html, final_url = await try_https_then_http(session, url)
        if not html:
            result["error"] = "Site not reachable"
            return result

        contact_page = await find_contact_page(session, final_url, html)
        result["contact_page"] = contact_page

        emails1, phones1 = extract_contacts(html)

        if contact_page != final_url:
            contact_html, _ = await fetch_html(session, contact_page)
            emails2, phones2 = extract_contacts(contact_html)
            result["emails"] = list(set(emails1 + emails2))
            result["phones"] = list(set(phones1 + phones2))
        else:
            result["emails"] = emails1
            result["phones"] = phones1

    except Exception as e:
        result["error"] = str(e)

    return result

# ----------- Bulk CSV Extraction ------------ #

async def extract_contacts_bulk(urls):
    os.makedirs("results", exist_ok=True)
    filename = f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join("results", filename)

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        results = []
        for url in urls:
            result = await scrape_site(session, url.strip())
            results.append(result)
            await asyncio.sleep(0.1)  # Prevent overload

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Contact Page", "Emails", "Phones", "Error"])
        for r in results:
            writer.writerow([
                r["url"],
                r.get("contact_page", ""),
                ", ".join(r.get("emails", [])),
                ", ".join(r.get("phones", [])),
                r.get("error", "")
            ])
    return filename

# ----------- API Routes ------------ #

@app.get("/extract")
async def extract_single(url: str = Query(...)):
    async with aiohttp.ClientSession(headers=HEADERS) as session:
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
