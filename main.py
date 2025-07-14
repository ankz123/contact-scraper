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
from playwright.async_api import async_playwright

app = FastAPI()

EMAIL_REGEX = re.compile(r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}")
PHONE_REGEX = re.compile(r"(?:\+91[-\s]?|\b)([6-9]\d{9})\b")
JUNK_EMAIL_DOMAINS = {"sentry.wixpress.com", "sentry.io", "sentry-next.wixpress.com"}

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def extract_contacts(html: str):
    emails, phones = set(), set()
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
            phone = re.sub(r"\D", "", href[4:])
            if len(phone) == 10:
                phones.add("+91" + phone)

    for phone in PHONE_REGEX.findall(text):
        phones.add("+91" + phone.strip())

    return list(emails), list(phones)

async def get_html(session, url):
    try:
        async with session.get(url, timeout=10) as r:
            return await r.text(), str(r.url)
    except:
        return None, url

async def get_html_playwright(url):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto(url, timeout=10000)
            html = await page.content()
            await browser.close()
            return html
    except:
        return None

async def find_contact_page(html, base_url):
    if not html:
        return base_url
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if "contact" in href:
            return urljoin(base_url, a["href"])
    return base_url

async def try_all_variants(base_url):
    parsed = base_url.replace("http://", "").replace("https://", "").replace("www.", "").strip("/")
    prefixes = ["http://", "https://"]
    subs = ["", "www."]
    variants = [f"{p}{s}{parsed}" for p in prefixes for s in subs]

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        for url in variants:
            html, final_url = await get_html(session, url)
            if html:
                return html, final_url
    return None, base_url

async def scrape_site(session, url: str):
    result = {"url": url, "emails": [], "phones": [], "error": None, "contact_page": None}

    html, final_url = await try_all_variants(url)
    if not html:
        html = await get_html_playwright(url)
        if not html:
            result["error"] = "Site not reachable"
            return result
        final_url = url

    contact_page = await find_contact_page(html, final_url)
    result["contact_page"] = contact_page

    emails1, phones1 = extract_contacts(html)
    if contact_page != final_url:
        html_contact, _ = await get_html(session, contact_page)
        if not html_contact:
            html_contact = await get_html_playwright(contact_page)
        emails2, phones2 = extract_contacts(html_contact)
        result["emails"] = list(set(emails1 + emails2))
        result["phones"] = list(set(phones1 + phones2))
    else:
        result["emails"] = emails1
        result["phones"] = phones1

    return result

async def extract_contacts_bulk(urls):
    os.makedirs("results", exist_ok=True)
    filename = f"results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join("results", filename)

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        results = await asyncio.gather(*[scrape_site(session, u) for u in urls if u.strip()])

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
