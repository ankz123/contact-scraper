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

app = FastAPI()

EMAIL_REGEX = re.compile(r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}")
JUNK_EMAIL_DOMAINS = {"sentry.wixpress.com", "sentry.io", "sentry-next.wixpress.com"}

# Universal phone number pattern
PHONE_REGEX = re.compile(
    r"""
    (?:
        (?:\+|00)?             # + or 00 prefix (optional)
        \d{1,3}                # Country code
        [\s\-\.]*
    )?
    (?:
        \(?\d{2,4}\)?          # Area code (optional parentheses)
        [\s\-\.]*
    )?
    \d{3,4}                   # First part
    [\s\-\.]*
    \d{3,4}                   # Second part
    """,
    re.VERBOSE
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}


async def fetch_html(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, timeout=10) as response:
            return await response.text(), str(response.url)
    except:
        return None, url


def clean_phone(phone: str) -> str:
    digits = re.sub(r"[^\d]", "", phone)
    if len(digits) >= 10:
        return "+" + digits if not digits.startswith("+") else digits
    return None


def extract_contacts(html: str):
    emails = set()
    phones = set()

    if not html:
        return [], []

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()

    # Find emails
    for match in EMAIL_REGEX.findall(text):
        domain = match.split("@")[-1]
        if domain not in JUNK_EMAIL_DOMAINS:
            emails.add(match.strip())

    for match in soup.find_all("a", href=True):
        href = match["href"]
        if href.startswith("mailto:"):
            email = href[7:].split("?")[0]
            domain = email.split("@")[-1]
            if domain not in JUNK_EMAIL_DOMAINS:
                emails.add(email.strip())
        elif href.startswith("tel:"):
            cleaned = clean_phone(href[4:])
            if cleaned:
                phones.add(cleaned)

    # Find phone numbers in visible text
    for match in PHONE_REGEX.findall(text):
        cleaned = clean_phone(match)
        if cleaned:
            phones.add(cleaned)

    return list(emails), list(phones)


async def find_contact_page(session, base_url, html):
    if not html:
        return base_url

    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        href = link["href"].lower()
        if any(keyword in href for keyword in ["contact", "contact-us"]):
            return urljoin(base_url, link["href"])

    return base_url


async def scrape_site(session: aiohttp.ClientSession, url: str):
    result = {"url": url, "emails": [], "phones": [], "error": None, "contact_page": None}

    try:
        html, final_url = await fetch_html(session, url)
        if not html:
            result["error"] = "Site not reachable"
            return result

        contact_page = await find_contact_page(session, url, html)
        result["contact_page"] = contact_page

        # Scrape main page and contact page if different
        emails1, phones1 = extract_contacts(html)
        if contact_page != final_url:
            html_contact, _ = await fetch_html(session, contact_page)
            emails2, phones2 = extract_contacts(html_contact)
            result["emails"] = list(set(emails1 + emails2))
            result["phones"] = list(set(phones1 + phones2))
        else:
            result["emails"] = emails1
            result["phones"] = phones1

    except Exception as e:
        result["error"] = str(e)

    return result


async def extract_contacts_bulk(urls):
    os.makedirs("results", exist_ok=True)
    filename = f"results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join("results", filename)

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = []
        batch_size = 75
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i+batch_size]
            tasks += [scrape_site(session, url.strip()) for url in batch]
            await asyncio.sleep(0.5)

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
