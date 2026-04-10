"""
TrendPulse - Task 3: Data Analysis
Loads the cleaned CSV from Task 2 and performs analysis using NumPy and Pandas.
Output: data/analysis_YYYYMMDD.json (summary stats saved for Task 4)
"""

import pandas as pd
import numpy as np
import json
import os
import glob
from datetime import datetime

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

DATE_STR     = datetime.now().strftime("%Y%m%d")
OUTPUT_FILE  = f"data/analysis_{DATE_STR}.json"


# ──────────────────────────────────────────────
# Step 1: Load the cleaned CSV
# ──────────────────────────────────────────────

def load_csv():
    """
    Finds the most recent cleaned CSV file produced by Task 2.
    Returns a Pandas DataFrame.
    """
    pattern = "data/trends_clean_????????.csv"
    matches = sorted(glob.glob(pattern))

    if not matches:
        print("[ERROR] No cleaned CSV found. Run task2_clean_csv.py first.")
        exit(1)

    filepath = matches[-1]
    print(f"Loading: {filepath}")

    # Read CSV into a DataFrame
    df = pd.read_csv(filepath)

    print(f"  Shape: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"  Columns: {list(df.columns)}\n")
    return df


# ──────────────────────────────────────────────
# Step 2: Basic data inspection
# ──────────────────────────────────────────────

def inspect(df):
    """
    Prints a quick overview of the DataFrame:
    data types, null counts, and basic numeric stats.
    """
    print("=== Data Types & Null Counts ===")
    print(df.dtypes)
    print()
    print(f"Null values per column:\n{df.isnull().sum()}\n")

    print("=== Basic Numeric Stats (Pandas describe) ===")
    # Only describe numeric columns (score, num_comments)
    print(df[["score", "num_comments"]].describe())
    print()


# ──────────────────────────────────────────────
# Step 3: NumPy analysis on scores
# ──────────────────────────────────────────────

def numpy_analysis(df):
    """
    Uses NumPy directly on the score and num_comments arrays.
    Demonstrates: mean, median, std, min, max, percentiles.
    """
    print("=== NumPy Analysis: Scores ===")

    scores = df["score"].to_numpy()          # convert Pandas Series → NumPy array
    comments = df["num_comments"].to_numpy()

    print(f"  Total stories      : {len(scores)}")
    print(f"  Mean score         : {np.mean(scores):.2f}")
    print(f"  Median score       : {np.median(scores):.2f}")
    print(f"  Std deviation      : {np.std(scores):.2f}")
    print(f"  Min score          : {np.min(scores)}")
    print(f"  Max score          : {np.max(scores)}")
    print(f"  25th percentile    : {np.percentile(scores, 25):.2f}")
    print(f"  75th percentile    : {np.percentile(scores, 75):.2f}")
    print()

    print("=== NumPy Analysis: Comments ===")
    print(f"  Mean comments      : {np.mean(comments):.2f}")
    print(f"  Median comments    : {np.median(comments):.2f}")
    print(f"  Max comments       : {np.max(comments)}")
    print()

    # Return stats dict so we can save it later
    return {
        "scores": {
            "mean":   round(float(np.mean(scores)), 2),
            "median": round(float(np.median(scores)), 2),
            "std":    round(float(np.std(scores)), 2),
            "min":    int(np.min(scores)),
            "max":    int(np.max(scores)),
            "p25":    round(float(np.percentile(scores, 25)), 2),
            "p75":    round(float(np.percentile(scores, 75)), 2),
        },
        "comments": {
            "mean":   round(float(np.mean(comments)), 2),
            "median": round(float(np.median(comments)), 2),
            "max":    int(np.max(comments)),
        }
    }


# ──────────────────────────────────────────────
# Step 4: Category-level analysis (Pandas groupby)
# ──────────────────────────────────────────────

def category_analysis(df):
    """
    Groups stories by category and calculates:
    - Story count per category
    - Average score per category
    - Average comments per category
    - Total score per category
    """
    print("=== Category Analysis (Pandas groupby) ===")

    # groupby + agg lets us compute multiple stats at once
    cat_stats = df.groupby("category").agg(
        story_count  = ("post_id",      "count"),
        avg_score    = ("score",        "mean"),
        avg_comments = ("num_comments", "mean"),
        total_score  = ("score",        "sum"),
        max_score    = ("score",        "max"),
    ).round(2)

    # Sort by average score descending so the "hottest" category is first
    cat_stats = cat_stats.sort_values("avg_score", ascending=False)

    print(cat_stats.to_string())
    print()

    # Convert to a plain dict for JSON serialisation later
    return cat_stats.reset_index().to_dict(orient="records")


# ──────────────────────────────────────────────
# Step 5: Top stories per category
# ──────────────────────────────────────────────

def top_stories(df, n=5):
    """
    Returns the top N stories (by score) for each category.
    Uses Pandas groupby + apply with nlargest.
    """
    print(f"=== Top {n} Stories Per Category (by score) ===")

    top = (
        df.groupby("category", group_keys=False)
          .apply(lambda g: g.nlargest(n, "score"))   # nlargest is efficient NumPy-backed sort
          [["category", "score", "num_comments", "title", "author"]]
    )

    print(top.to_string(index=False))
    print()

    # Return as list of dicts for JSON output
    return top.to_dict(orient="records")


# ──────────────────────────────────────────────
# Step 6: Most active authors
# ──────────────────────────────────────────────

def author_analysis(df, n=10):
    """
    Finds the most prolific authors in the dataset.
    Also calculates their average score — high count + high avg = influential.
    """
    print(f"=== Top {n} Most Active Authors ===")

    author_stats = (
        df[df["author"] != "unknown"]          # exclude placeholder authors
          .groupby("author")
          .agg(
              post_count = ("post_id", "count"),
              avg_score  = ("score",   "mean"),
              total_score= ("score",   "sum"),
          )
          .round(2)
          .sort_values("post_count", ascending=False)
          .head(n)
    )

    print(author_stats.to_string())
    print()

    return author_stats.reset_index().to_dict(orient="records")


# ──────────────────────────────────────────────
# Step 7: Score distribution buckets (NumPy histogram)
# ──────────────────────────────────────────────

def score_distribution(df):
    """
    Uses NumPy histogram to bucket stories by score range.
    This gives Task 4 the data it needs to draw a bar chart.
    """
    print("=== Score Distribution (NumPy histogram) ===")

    scores = df["score"].to_numpy()

    # 6 equal-width bins across the score range
    counts, bin_edges = np.histogram(scores, bins=6)

    print(f"  {'Score range':<20} {'Count':>6}")
    print(f"  {'-'*28}")

    buckets = []
    for i in range(len(counts)):
        low  = int(bin_edges[i])
        high = int(bin_edges[i + 1])
        label = f"{low}–{high}"
        print(f"  {label:<20} {counts[i]:>6}")
        buckets.append({"range": label, "count": int(counts[i])})

    print()
    return buckets


# ──────────────────────────────────────────────
# Step 8: Correlation (NumPy)
# ──────────────────────────────────────────────

def correlation_analysis(df):
    """
    Uses NumPy to calculate the Pearson correlation between
    score and number of comments.
    A high correlation means popular posts also get more discussion.
    """
    print("=== Correlation: Score vs Comments (NumPy) ===")

    scores   = df["score"].to_numpy()
    comments = df["num_comments"].to_numpy()

    # np.corrcoef returns a 2x2 matrix; [0][1] is the cross-correlation
    corr_matrix = np.corrcoef(scores, comments)
    corr        = corr_matrix[0][1]

    print(f"  Pearson correlation (score vs comments): {corr:.4f}")

    if corr > 0.7:
        print("  → Strong positive correlation.")
    elif corr > 0.4:
        print("  → Moderate positive correlation.")
    elif corr > 0:
        print("  → Weak positive correlation.")
    else:
        print("  → Weak or no positive correlation.")

    print()
    return round(float(corr), 4)


# ──────────────────────────────────────────────
# Step 9: Save analysis results to JSON
# ──────────────────────────────────────────────

def save_analysis(results):
    """
    Saves all computed stats to a JSON file for use in Task 4 (visualisation).
    """
    os.makedirs("data", exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"Analysis saved to {OUTPUT_FILE}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    print("=== TrendPulse — Task 3: Data Analysis ===\n")

    # Step 1 — Load cleaned CSV
    df = load_csv()

    # Step 2 — Quick inspection
    inspect(df)

    # Step 3 — NumPy stats on scores and comments
    global_stats = numpy_analysis(df)

    # Step 4 — Category-level groupby analysis
    cat_stats = category_analysis(df)

    # Step 5 — Top 5 stories per category
    top = top_stories(df, n=5)

    # Step 6 — Most active authors
    authors = author_analysis(df, n=10)

    # Step 7 — Score distribution buckets
    distribution = score_distribution(df)

    # Step 8 — Correlation between score and comments
    correlation = correlation_analysis(df)

    # Step 9 — Bundle everything and save for Task 4
    results = {
        "generated_at":    datetime.now().isoformat(),
        "total_stories":   len(df),
        "global_stats":    global_stats,
        "category_stats":  cat_stats,
        "top_stories":     top,
        "top_authors":     authors,
        "score_distribution": distribution,
        "score_comment_correlation": correlation,
    }

    save_analysis(results)
    print(f"\nDone. {len(df)} stories analysed.")


if __name__ == "__main__":
    main()
