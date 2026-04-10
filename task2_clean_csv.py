"""
TrendPulse - Task 2: Data Cleaning
Reads the JSON file produced by Task 1, cleans it, and saves it as a CSV.
Output: data/trends_clean_YYYYMMDD.csv
"""

import json
import csv
import os
import glob
from datetime import datetime

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

# Output CSV path
DATE_STR    = datetime.now().strftime("%Y%m%d")
OUTPUT_FILE = f"data/trends_clean_{DATE_STR}.csv"

# These are the only fields we keep (in this column order)
FIELDS = ["post_id", "title", "category", "score", "num_comments", "author", "collected_at"]

# Valid category names — anything else is dropped
VALID_CATEGORIES = {"technology", "worldnews", "sports", "science", "entertainment"}


# ──────────────────────────────────────────────
# Step 1: Find and load the JSON file
# ──────────────────────────────────────────────

def load_json():
    """
    Looks for any trends_YYYYMMDD.json file inside the data/ folder.
    Uses the most recently created one if multiple exist.
    Returns the loaded list of story dicts, or exits if none found.
    """
    # Glob pattern to find all raw JSON files from Task 1
    pattern = "data/trends_????????.json"
    matches = sorted(glob.glob(pattern))  # sorted = oldest first

    if not matches:
        print("[ERROR] No JSON file found in data/. Run task1_data_collection.py first.")
        exit(1)

    # Pick the most recent file
    filepath = matches[-1]
    print(f"Loading: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"  Loaded {len(data)} raw records.")
    return data


# ──────────────────────────────────────────────
# Step 2: Clean each record
# ──────────────────────────────────────────────

def clean_record(record):
    """
    Cleans a single story record.
    Returns a cleaned dict, or None if the record should be dropped entirely.

    Cleaning rules applied:
    - Drop record if post_id or title is missing
    - Drop record if category is not one of the 5 valid ones
    - Strip whitespace from string fields
    - Ensure score and num_comments are integers (default 0 if missing/invalid)
    - Truncate very long titles to 300 characters
    - Fill missing author with 'unknown'
    - Fill missing collected_at with empty string
    """

    # Must have a post_id — otherwise we can't identify the record
    if not record.get("post_id"):
        return None

    # Must have a title — titleless stories are useless
    title = str(record.get("title", "")).strip()
    if not title:
        return None

    # Category must be one of our 5 valid ones
    category = str(record.get("category", "")).strip().lower()
    if category not in VALID_CATEGORIES:
        return None

    # Clean score: cast to int, default 0 if missing or non-numeric
    try:
        score = int(record.get("score", 0))
    except (ValueError, TypeError):
        score = 0

    # Clean num_comments: same approach
    try:
        num_comments = int(record.get("num_comments", 0))
    except (ValueError, TypeError):
        num_comments = 0

    # Negative scores/comments don't make sense — floor at 0
    score        = max(score, 0)
    num_comments = max(num_comments, 0)

    # Clean author: strip whitespace, default to 'unknown'
    author = str(record.get("author", "")).strip()
    if not author:
        author = "unknown"

    # Truncate extremely long titles (edge case but good practice)
    if len(title) > 300:
        title = title[:300] + "..."

    # collected_at: keep as-is (it's an ISO timestamp string)
    collected_at = str(record.get("collected_at", "")).strip()

    return {
        "post_id":      record["post_id"],
        "title":        title,
        "category":     category,
        "score":        score,
        "num_comments": num_comments,
        "author":       author,
        "collected_at": collected_at,
    }


def clean_all(records):
    """
    Runs clean_record() on every record, then removes duplicates by post_id.
    Returns the final cleaned list.
    """
    cleaned = []
    dropped = 0

    for record in records:
        result = clean_record(record)
        if result is None:
            dropped += 1
        else:
            cleaned.append(result)

    print(f"  After cleaning : {len(cleaned)} records kept, {dropped} dropped.")

    # Remove duplicate post_ids — keep the first occurrence
    seen     = set()
    deduped  = []
    for record in cleaned:
        pid = record["post_id"]
        if pid not in seen:
            seen.add(pid)
            deduped.append(record)

    duplicates_removed = len(cleaned) - len(deduped)
    if duplicates_removed:
        print(f"  Duplicates removed: {duplicates_removed}")

    print(f"  Final record count: {len(deduped)}")
    return deduped


# ──────────────────────────────────────────────
# Step 3: Save to CSV
# ──────────────────────────────────────────────

def save_csv(records):
    """
    Writes the cleaned records to a CSV file.
    Uses the FIELDS list as the column order.
    """
    os.makedirs("data", exist_ok=True)  # ensure folder exists

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()             # write column names as first row
        writer.writerows(records)        # write all data rows

    print(f"\nCleaned data saved to {OUTPUT_FILE}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    print("=== TrendPulse — Task 2: Data Cleaning ===\n")

    # Step 1 — Load raw JSON from Task 1
    raw_data = load_json()

    # Step 2 — Clean and deduplicate
    print("\nCleaning records...")
    clean_data = clean_all(raw_data)

    # Sanity check — warn if we ended up with fewer than 100 stories
    if len(clean_data) < 100:
        print(f"[WARN] Only {len(clean_data)} stories after cleaning (expected 100+).")

    # Step 3 — Save to CSV
    save_csv(clean_data)

    # Show per-category count in the final CSV
    print("\nBreakdown by category:")
    for cat in VALID_CATEGORIES:
        count = sum(1 for r in clean_data if r["category"] == cat)
        print(f"  {cat:15s}: {count}")


if __name__ == "__main__":
    main()
