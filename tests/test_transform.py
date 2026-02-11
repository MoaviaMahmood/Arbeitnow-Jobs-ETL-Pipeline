import pytest
from etl.transform import (
    normalize_missing,
    clean_slug,
    clean_company_name,
    clean_title,
    clean_description,
    clean_remote,
    clean_location,
    clean_created_at,
    clean_job_record
)

# -------------------------
# normalize_missing
# -------------------------

def test_normalize_missing_none():
    assert normalize_missing(None) == "unknown"

def test_normalize_missing_empty_string():
    assert normalize_missing("   ") == "unknown"

def test_normalize_missing_valid():
    assert normalize_missing("Berlin") == "Berlin"


# -------------------------
# clean_slug
# -------------------------

def test_clean_slug_valid():
    slug_title, job_id = clean_slug("backend-developer-berlin-12345")
    assert slug_title == "backend developer berlin"
    assert job_id == "12345"

def test_clean_slug_none():
    slug_title, job_id = clean_slug(None)
    assert slug_title is None
    assert job_id is None


# -------------------------
# clean_company_name
# -------------------------

def test_clean_company_encoding():
    result = clean_company_name("Rohrleitungsbau MÃ¼nster GmbH")
    assert "Münster" in result


# -------------------------
# clean_title
# -------------------------

def test_clean_title_encoding():
    result = clean_title("Java Entwickler fÃ¼r Backend")
    assert "für" in result

def test_clean_title_remote_standardization():
    result = clean_title("100% REMOTE Developer")
    assert result == "100% Remote Developer"


# -------------------------
# clean_description
# -------------------------

def test_clean_description_encoding():
    text = "Du mÃ¶chtest Backend entwickeln"
    cleaned = clean_description(text)
    assert "möchtest" in cleaned

def test_clean_description_html_removal():
    text = "<p>Hello <strong>World</strong></p>"
    cleaned = clean_description(text)
    assert "Hello World" in cleaned
    assert "<" not in cleaned


# -------------------------
# clean_remote
# -------------------------

def test_clean_remote_true():
    assert clean_remote("true") == 1
    assert clean_remote(True) == 1

def test_clean_remote_false():
    assert clean_remote("false") == 0
    assert clean_remote("FASLE") == 0


# -------------------------
# clean_location
# -------------------------

def test_clean_location_valid():
    city, region, country = clean_location("Heidelberg, Baden-Württemberg, Germany")
    assert city == "Heidelberg"
    assert region == "Baden-Württemberg"
    assert country == "Germany"


# -------------------------
# clean_created_at
# -------------------------

def test_clean_created_at_valid():
    created_at, created_date, year, month = clean_created_at("1770827496")

    assert created_at.startswith("2026-02-11")
    assert created_date == "2026-02-11"
    assert year == 2026
    assert month == 2


# -------------------------
# clean_job_record integration
# -------------------------

def test_clean_job_record_full():
    sample_job = {
        "slug": "backend-developer-berlin-12345",
        "company_name": "Test GmbH",
        "title": "Java Entwickler",
        "description": "<p>Hello World</p>",
        "remote": "true",
        "url": "https://example.com",
        "location": "Berlin, Berlin, Germany",
        "created_at": "1770827496"
    }

    cleaned = clean_job_record(sample_job)

    assert cleaned["slug_title"] == "backend developer berlin"
    assert cleaned["job_id"] == "12345"
    assert cleaned["remote"] == 1
    assert cleaned["city"] == "Berlin"
    assert cleaned["created_year"] == 2026