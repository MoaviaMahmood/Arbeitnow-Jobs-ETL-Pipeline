import csv
import json
import re
import html
from datetime import datetime, UTC



def normalize_missing(value):
    if value is None:
        return "unknown"

    if isinstance(value, str):
        value = value.strip()
        if value == "" or value.lower() in ["missing value", "none", "null"]:
            return "unknown"

    return value


'''''''''''''''''''''''''''''''''
Clean column "slug"
'''''''''''''''''''''''''''''''''
def clean_slug(value):
    if not value:
        return None, None

    value = value.strip()

    # Extract job_id (numbers at end)
    match = re.search(r"-(\d+)$", value)
    job_id = match.group(1) if match else None

    # Remove job_id from slug
    slug_without_id = re.sub(r"-\d+$", "", value)

    # Convert hyphen to space
    slug_title = slug_without_id.replace("-", " ")

    return slug_title, job_id

'''''''''''''''''''''''''''''''''
Clean column "company_name"
'''''''''''''''''''''''''''''''''
def clean_company_name(value):
    if not value:
        return None

    value = value.strip()

    # Fix broken UTF-8 encoding (MÃ¼nster → Münster)
    try:
        value = value.encode("latin1").decode("utf-8")
    except Exception:
        pass

    # Normalize whitespace
    value = re.sub(r"\s+", " ", value)

    return value

'''''''''''''''''''''''''''''''''
Clean column "title"
'''''''''''''''''''''''''''''''''
def clean_title(value):
    if not value:
        return None

    value = value.strip()

    # Fix broken UTF-8 encoding (â€ž, fÃ¼r, etc.)
    try:
        value = value.encode("latin1").decode("utf-8")
    except Exception:
        pass

    # Decode HTML entities
    value = html.unescape(value)

    # Normalize whitespace
    value = re.sub(r"\s+", " ", value)

    # Standardize REMOTE formatting
    value = re.sub(r"100%\s*REMOTE", "100% Remote", value, flags=re.IGNORECASE)
    value = re.sub(r"homeoffice", "Home Office", value, flags=re.IGNORECASE)

    return value.strip()

'''''''''''''''''''''''''''''''''
Clean column "description"
'''''''''''''''''''''''''''''''''
def fix_encoding(text: str) -> str:
    """
    Fix common UTF-8 / Latin1 / CP1252 mis-encoding issues.
    Handles cases like:
    mÃ¶chtest -> möchtest
    GrÃ¼ÃŸe -> Grüße
    """
    if not text:
        return ""

    # First attempt: latin1 -> utf-8
    try:
        text = text.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass

    # Second attempt: cp1252 -> utf-8 (very common in German datasets)
    try:
        text = text.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass

    return text


def clean_description(text: str) -> str:
    """
    Cleans job description:
    - Fix encoding issues
    - Decode HTML entities
    - Preserve list bullets
    - Remove HTML tags
    - Remove Arbeitnow footer
    - Normalize whitespace
    """

    if not text:
        return ""

    # ---- 1. Fix encoding ----
    text = fix_encoding(text)

    # ---- 2. Decode HTML entities (&amp;, &uuml;, etc.)
    text = html.unescape(text)

    # ---- 3. Normalize weird bullet characters
    text = text.replace("â€¢", "•")
    text = text.replace("·", "•")

    # ---- 4. Preserve list structure
    text = re.sub(r"<li.*?>", "\n• ", text, flags=re.IGNORECASE)
    text = re.sub(r"</li>", "", text, flags=re.IGNORECASE)

    # ---- 5. Convert block elements to newline
    text = re.sub(r"</p>|</h\d>|</ul>|</ol>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)

    # ---- 6. Remove remaining HTML tags
    text = re.sub(r"<.*?>", "", text)

    # ---- 7. Remove Arbeitnow footer safely
    text = re.sub(
        r"Find\s+(more\s+)?jobs?\s+in\s+Germany\s+on\s+Arbeitnow.*",
        "",
        text,
        flags=re.IGNORECASE,
    )

    # ---- 8. Remove invisible unicode control chars
    text = re.sub(r"[\u200b-\u200f\u202a-\u202e]", "", text)

    # ---- 9. Normalize whitespace
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)

    return text.strip()

'''''''''''''''''''''''''''''''''
Clean column "remote"
'''''''''''''''''''''''''''''''''
def clean_remote(value):
    if isinstance(value, bool):
        return 1 if value else 0

    if value is None:
        return 0

    # Convert to string for safety
    value = str(value).strip().lower()

    # Fix common typo
    if value == "fasle":
        value = "false"

    if value in ["true", "1", "yes"]:
        return 1
    elif value in ["false", "0", "no"]:
        return 0
    else:
        return 0  # fallback

'''''''''''''''''''''''''''''''''
Clean column "location"
'''''''''''''''''''''''''''''''''
def clean_location(value):
    if not value:
        return None, None, None

    value = value.strip()

    # Fix broken UTF-8 encoding
    try:
        value = value.encode("latin1").decode("utf-8")
    except Exception:
        pass

    # Normalize whitespace
    value = re.sub(r"\s+", " ", value)

    parts = [p.strip() for p in value.split(",")]

    city = parts[0] if len(parts) > 0 else None
    region = parts[1] if len(parts) > 1 else None
    country = parts[2] if len(parts) > 2 else None

    return city, region, country

'''''''''''''''''''''''''''''''''
Clean column "created_at"
'''''''''''''''''''''''''''''''''
def clean_created_at(value):
    if not value:
        return None, None, None, None

    try:
        timestamp = int(value)
        dt = datetime.fromtimestamp(timestamp, UTC)

        created_at = dt.strftime("%Y-%m-%d %H:%M:%S")
        created_date = dt.strftime("%Y-%m-%d")
        created_year = dt.year
        created_month = dt.month

        return created_at, created_date, created_year, created_month

    except Exception:
        return None, None, None, None



def clean_job_record(job):
    cleaned = {}

    # ---- SLUG ----
    slug_title, job_id = clean_slug(job.get("slug"))
    cleaned["slug_title"] = normalize_missing(slug_title)
    cleaned["job_id"] = normalize_missing(job_id)

    # ---- COMPANY ----
    cleaned["company_name"] = normalize_missing(
        clean_company_name(job.get("company_name"))
    )

    # ---- TITLE ----
    cleaned["title"] = normalize_missing(
        clean_title(job.get("title"))
    )

    # ---- DESCRIPTION ----
    description = clean_description(job.get("description"))
    cleaned["description"] = normalize_missing(description)
    cleaned["description_length"] = len(description) if description else 0

    # ---- REMOTE ----
    cleaned["remote"] = clean_remote(job.get("remote"))

    # ---- URL ----
    cleaned["url"] = normalize_missing(job.get("url"))

    # ---- LOCATION ----
    city, region, country = clean_location(job.get("location"))

    cleaned["city"] = normalize_missing(city)
    cleaned["region"] = normalize_missing(region)
    cleaned["country"] = normalize_missing(country)

    # ---- CREATED ----
    created_at, created_date, created_year, created_month = clean_created_at(job.get("created_at"))

    cleaned["created_at"] = normalize_missing(created_at)
    cleaned["created_date"] = normalize_missing(created_date)
    cleaned["created_year"] = normalize_missing(created_year)
    cleaned["created_month"] = normalize_missing(created_month)


    return cleaned



def transform_bronze_to_silver(input_path, output_path):
    """
    Reads Bronze JSON file, cleans records,
    and writes Silver CSV file.
    """

    with open(input_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    jobs = payload.get("data", [])

    if not jobs:
        print("No jobs found in payload.")
        return 0

    cleaned_jobs = [clean_job_record(job) for job in jobs]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cleaned_jobs[0].keys())
        writer.writeheader()
        writer.writerows(cleaned_jobs)

    print(f"Silver CSV created successfully at {output_path}")
    print(f"Total records processed: {len(cleaned_jobs)}")

    return len(cleaned_jobs)
