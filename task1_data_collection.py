"""
TrendPulse - Task 1: Data Collection
Fetches trending stories from HackerNews API and categorises them
into 5 groups: technology, worldnews, sports, science, entertainment.
Output: data/trends_YYYYMMDD.json
"""

import requests
import json
import os
import time
from datetime import datetime

# ──────────────────────────────────────────────
# Category keyword map (case-insensitive matching)
# ──────────────────────────────────────────────
CATEGORIES = {
    "technology":    ["ai", "software", "tech", "code", "computer", "data", "cloud", "api", "gpu", "llm"],
    "worldnews":     ["war", "government", "country", "president", "election", "climate", "attack", "global"],
    "sports":        ["nfl", "nba", "fifa", "sport", "game", "team", "player", "league", "championship"],
    "science":       ["research", "study", "space", "physics", "biology", "discovery", "nasa", "genome"],
    "entertainment": ["movie", "film", "music", "netflix", "game", "book", "show", "award", "streaming"],
}

# Max stories to collect per category
MAX_PER_CATEGORY = 25

# How many top story IDs to fetch from HackerNews
TOP_STORIES_LIMIT = 500

# Standard header to identify our app politely
HEADERS = {"User-Agent": "TrendPulse/1.0"}

# Base URLs for the HackerNews Firebase API
TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
ITEM_URL        = "https://hacker-news.firebaseio.com/v0/item/{id}.json"


def fetch_top_story_ids():
    """
    Step 1: Fetch the list of top story IDs from HackerNews.
    Returns the first 500 IDs, or an empty list if the request fails.
    """
    try:
        response = requests.get(TOP_STORIES_URL, headers=HEADERS, timeout=10)
        response.raise_for_status()          # raises HTTPError for 4xx/5xx
        ids = response.json()
        print(f"Fetched {len(ids)} story IDs. Using first {TOP_STORIES_LIMIT}.")
        return ids[:TOP_STORIES_LIMIT]
    except requests.RequestException as e:
        print(f"[ERROR] Could not fetch top story IDs: {e}")
        return []


def fetch_story(story_id):
    """
    Step 2: Fetch details for a single story by its ID.
    Returns the story dict, or None if the request fails.
    """
    try:
        url = ITEM_URL.format(id=story_id)
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        # Don't crash — just log and skip this story
        print(f"  [WARN] Could not fetch story {story_id}: {e}")
        return None


def assign_category(title):
    """
    Determine which category a story belongs to by scanning its title
    for category keywords (case-insensitive).
    Returns the matching category name, or None if no match is found.
    """
    if not title:
        return None

    title_lower = title.lower()

    for category, keywords in CATEGORIES.items():
        for keyword in keywords:
            # Use 'in' for substring match (e.g. "AI" inside "AI startup")
            if keyword.lower() in title_lower:
                return category

    return None   # title didn't match any category


def extract_fields(story, category):
    """
    Pull the 7 required fields out of a raw HackerNews story object.
    'descendants' holds the comment count; it may be absent on new posts.
    'collected_at' is added by us to record when we scraped this story.
    """
    return {
        "post_id":      story.get("id"),
        "title":        story.get("title", ""),
        "category":     category,
        "score":        story.get("score", 0),
        "num_comments": story.get("descendants", 0),   # may be missing → default 0
        "author":       story.get("by", ""),
        "collected_at": datetime.now().isoformat(),     # e.g. "2024-01-15T14:23:01.123456"
    }


def collect_stories(story_ids):
    """
    Main collection loop.
    Iterates over all fetched story IDs, assigns each story a category,
    and fills up buckets (max 25 per category).
    Sleeps 2 seconds after processing each category's quota is filled.
    Returns a flat list of all collected story dicts.
    """
    # One list per category — we stop adding once a bucket hits 25
    buckets = {cat: [] for cat in CATEGORIES}

    # Track which categories still have room (avoids re-checking full ones)
    open_categories = set(CATEGORIES.keys())

    print("\nFetching individual stories...")

    for story_id in story_ids:
        # If every category is full, we're done — no need to keep fetching
        if not open_categories:
            print("All category buckets full. Stopping early.")
            break

        story = fetch_story(story_id)

        # Skip if fetch failed, or story isn't a regular post (e.g. job ads)
        if not story or story.get("type") != "story":
            continue

        title = story.get("title", "")
        category = assign_category(title)

        # Skip stories that don't match any category
        if category is None:
            continue

        # Skip if this category's bucket is already full
        if category not in open_categories:
            continue

        # Extract and store the required fields
        record = extract_fields(story, category)
        buckets[category].append(record)

        print(f"  [{category}] ({len(buckets[category])}/25) — {title[:70]}")

        # When a category just hit 25 stories, mark it closed and sleep 2s
        if len(buckets[category]) >= MAX_PER_CATEGORY:
            open_categories.discard(category)
            print(f"  → Category '{category}' complete. Sleeping 2s...\n")
            time.sleep(2)   # one sleep per category, as specified

    # Flatten all buckets into a single list
    all_stories = []
    for cat_stories in buckets.values():
        all_stories.extend(cat_stories)

    return all_stories


def save_to_json(stories):
    """
    Step 3: Save collected stories to data/trends_YYYYMMDD.json.
    Creates the data/ folder if it doesn't already exist.
    """
    # Create output directory if missing
    os.makedirs("data", exist_ok=True)

    # Build dated filename, e.g. data/trends_20240115.json
    date_str  = datetime.now().strftime("%Y%m%d")
    filename  = f"data/trends_{date_str}.json"

    with open(filename, "w", encoding="utf-8") as f:
        # indent=2 makes the file human-readable
        json.dump(stories, f, indent=2, ensure_ascii=False)

    return filename


def main():
    print("=== TrendPulse — Task 1: Data Collection ===\n")

    # Step 1 — Get the top story IDs
    story_ids = fetch_top_story_ids()
    if not story_ids:
        print("No story IDs retrieved. Exiting.")
        return

    # Step 2 — Fetch stories and sort into categories
    stories = collect_stories(story_ids)

    # Step 3 — Save to JSON
    if stories:
        filepath = save_to_json(stories)
        print(f"\nCollected {len(stories)} stories. Saved to {filepath}")
    else:
        print("\n[ERROR] No stories were collected. JSON file not created.")

    # Show per-category breakdown for convenience
    print("\nBreakdown by category:")
    for cat in CATEGORIES:
        count = sum(1 for s in stories if s["category"] == cat)
        print(f"  {cat:15s}: {count}")


if __name__ == "__main__":
    main()
