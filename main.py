import os
import re
import csv
import asyncio
import pandas as pd
from urllib.parse import urljoin, urlparse

from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel

from bs4 import BeautifulSoup
import aiohttp
from playwright.async_api import async_playwright

# Auto-install Chromium in Render
if not os.path.exists("/tmp/chromium-installed"):
    from playwright.__main__ import main as playwright_main
    try:
        playwright_main(["install", "chromium"])
        with open("/tmp/chromium-installed", "w") as f:
            f.write("done")
    except Exception as e:
        print("Playwright install failed:", e)

# --- CONFIG ---
EMAIL_REGEX = re.compile(r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}")
PHONE_REGEX = re.compile(r"(?:\+91[-\s]?|\b)([6-9]\d{9})\b")
JUNK_EMAIL_DOMAINS = {"sentry.wixpress.com", "sentry.io", "sentry-next.wixpress.com"}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
}

app = FastAPI()


# --- HTML Scraping Helpers ---

def extract_contacts_from_html(html: str):
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
            phone = href[4:].strip()
            if phone.isdigit() and len(phone) == 10:
                phones.add("+91" + phone)

    for phone in PHONE_REGEX.findall(text):
        phones.add("+91" + phone.strip())

    return list(emails), list(phones)


async def fetch_html_aiohttp(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, timeout=10) as response:
            return await response.text(), str(response.url)
    except:
        return None, url


async def fetch_html_playwright(url: str):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, timeout=20000)
            content = await page.content()
            final_url = page.url
            await browser.close()
            return content, final_url
    except:
        return None, url


async def find_contact_page(html, base_url):
    if not html:
        return base_url

    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if "contact" in href:
            return urljoin(base_url, a["href"])

    return base_url


async def scrape_site(session, url: str):
    result = {
        "url": url,
        "emails": [],
        "phones": [],
        "contact_page": None,
        "error": None,
    }

    try:
        if not url.startswith("http"):
            url = "http://" + url

        # Try Playwright first
        html, final_url = await fetch_html_playwright(url)
        if not html:
            # fallback to aiohttp
            html, final_url = await fetch_html_aiohttp(session, url)

        if not html:
            result["error"] = "Site not reachable"
            return result

        contact_page = await find_contact_page(html, final_url)
        result["contact_page"] = contact_page

        emails1, phones1 = extract_contacts_from_html(html)

        if contact_page != final_url:
            contact_html, _ = await fetch_html_playwright(contact_page)
            if not contact_html:
                contact_html, _ = await fetch_html_aiohttp(session, contact_page)

            emails2, phones2 = extract_contacts_from_html(contact_html)
            result["emails"] = list(set(emails1 + emails2))
            result["phones"] = list(set(phones1 + phones2))
        else:
            result["emails"] = emails1
            result["phones"] = phones1

    except Exception as e:
        result["error"] = str(e)

    return result


# --- Bulk Handler ---

async def extract_contacts_bulk(urls):
    os.makedirs("results", exist_ok=True)
    filename = f"results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join("results", filename)

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        sem = asyncio.Semaphore(10)  # concurrency limit

        async def safe_scrape(url):
            async with sem:
                return await scrape_site(session, url.strip())

        tasks = [safe_scrape(url) for url in urls if url.strip()]
        results = await asyncio.gather(*tasks)

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


# --- API Routes ---

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
