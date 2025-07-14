import os
import re
import csv
import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from urllib.parse import urljoin
from playwright.async_api import async_playwright

app = FastAPI()

EMAIL_REGEX = re.compile(r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}")
PHONE_REGEX = re.compile(r"(?:\+91[-\s]?|\b)([6-9]\d{9})\b")
JUNK_EMAIL_DOMAINS = {"sentry.wixpress.com", "sentry.io", "sentry-next.wixpress.com"}

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

MAX_CONCURRENCY = 20
RETRIES = 2


def extract_contacts(html: str):
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

    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if href.startswith("mailto:"):
            email = href[7:].split("?")[0]
            domain = email.split("@")[-1]
            if domain not in JUNK_EMAIL_DOMAINS:
                emails.add(email.strip())
        elif href.startswith("tel:"):
            number = re.sub(r"[^\d]", "", href[4:])
            if len(number) == 10:
                phones.add("+91" + number)

    for number in PHONE_REGEX.findall(text):
        phones.add("+91" + number.strip())

    return list(emails), list(phones)


async def fetch_html(session, url):
    try:
        async with session.get(url, timeout=10) as response:
            return await response.text(), str(response.url)
    except:
        return None, url


async def fetch_with_playwright(url):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, timeout=15000)
            content = await page.content()
            await browser.close()
            return content, url
    except:
        return None, url


async def find_contact_page(session, base_url, html):
    if not html:
        return base_url

    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        href = link["href"].lower()
        if any(keyword in href for keyword in ["contact", "contact-us"]):
            return urljoin(base_url, link["href"])
    return base_url


async def scrape_site(session, url, semaphore):
    result = {"url": url, "emails": [], "phones": [], "error": None, "contact_page": None}

    async with semaphore:
        for attempt in range(RETRIES + 1):
            try:
                html, final_url = await fetch_html(session, url)
                if not html:
                    html, final_url = await fetch_with_playwright(url)
                if not html:
                    result["error"] = "Site not reachable"
                    return result

                contact_page = await find_contact_page(session, url, html)
                result["contact_page"] = contact_page

                emails1, phones1 = extract_contacts(html)

                if contact_page != final_url:
                    html2, _ = await fetch_html(session, contact_page)
                    if not html2:
                        html2, _ = await fetch_with_playwright(contact_page)
                    emails2, phones2 = extract_contacts(html2)
                    result["emails"] = list(set(emails1 + emails2))
                    result["phones"] = list(set(phones1 + phones2))
                else:
                    result["emails"] = emails1
                    result["phones"] = phones1

                return result
            except Exception as e:
                result["error"] = str(e)
                if attempt == RETRIES:
                    return result
                await asyncio.sleep(1)


async def extract_contacts_bulk(urls):
    os.makedirs("results", exist_ok=True)
    filename = f"results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join("results", filename)

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = [scrape_site(session, url.strip(), semaphore) for url in urls if url.strip()]
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


@app.get("/extract")
async def extract_single(url: str = Query(...)):
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        return await scrape_site(session, url, semaphore)


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
