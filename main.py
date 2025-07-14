from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from urllib.parse import urljoin
import httpx, re, csv, uuid, os, asyncio
from selectolax.parser import HTMLParser

app = FastAPI()

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
PHONE_REGEX = re.compile(r"\b(?:\+91[-\s]?|0)?([6-9]\d{9})\b")

JUNK_EMAIL_DOMAINS = ["sentry.wixpress.com", "sentry.io", "wixpress.com"]

class BulkInput(BaseModel):
    urls: list[str]

def normalize_phone(num: str) -> str:
    return "+91" + num[-10:]

async def fetch_html(url, session):
    try:
        r = await session.get(url, timeout=15)
        if r.status_code == 200:
            return r.text
    except:
        return None

async def extract_from_url(url):
    result = {
        "url": url,
        "contact_page": None,
        "emails": [],
        "phones": [],
        "error": None
    }

    try:
        async with httpx.AsyncClient(follow_redirects=True) as session:
            home_html = await fetch_html(url, session)
            if not home_html:
                result["error"] = "Homepage failed to load"
                return result

            parser = HTMLParser(home_html)
            for a in parser.css("a"):
                href = a.attributes.get("href", "")
                text = a.text().lower()
                if "contact" in href or "contact" in text:
                    result["contact_page"] = urljoin(url, href)
                    break

            final_url = result["contact_page"] or url
            contact_html = await fetch_html(final_url, session)
            if not contact_html:
                result["error"] = "Contact page failed to load"
                return result

            parsed = HTMLParser(contact_html)
            text = parsed.body.text()

            # Emails
            email_links = [a.attributes["href"].replace("mailto:", "") for a in parsed.css("a[href^='mailto:']")]
            email_texts = EMAIL_REGEX.findall(text)
            raw_emails = list(set(email_links + email_texts))
            result["emails"] = [
                e for e in raw_emails
                if not any(junk in e for junk in JUNK_EMAIL_DOMAINS)
            ]

            # Phones
            phone_links = [normalize_phone(a.attributes["href"].replace("tel:", "")) for a in parsed.css("a[href^='tel:']")]
            phone_texts = [normalize_phone(p) for p in PHONE_REGEX.findall(text)]
            result["phones"] = list(set(phone_links + phone_texts))
    except Exception as e:
        result["error"] = str(e)

    return result

@app.get("/extract")
async def extract(url: str = Query(...)):
    return await extract_from_url(url)

@app.post("/extract/bulk")
async def extract_bulk(data: BulkInput):
    urls = data.urls
    results = await asyncio.gather(*(extract_from_url(url) for url in urls))

    # Retry failed
    failed = [r["url"] for r in results if r["error"]]
    if failed:
        retry_results = await asyncio.gather(*(extract_from_url(url) for url in failed))
        for retry in retry_results:
            for i, r in enumerate(results):
                if r["url"] == retry["url"] and r["error"]:
                    results[i] = retry

    # Write to CSV
    filename = f"results_{uuid.uuid4().hex}.csv"
    filepath = f"/tmp/{filename}"
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Contact Page", "Emails", "Phones", "Error"])
        for r in results:
            writer.writerow([
                r["url"],
                r["contact_page"] or "",
                ", ".join(r["emails"]),
                ", ".join(r["phones"]),
                r["error"] or ""
            ])

    return {"results": results, "csv_url": f"/download/{filename}"}

@app.get("/download/{filename}")
async def download(filename: str):
    return FileResponse(f"/tmp/{filename}", media_type="text/csv", filename=filename)
