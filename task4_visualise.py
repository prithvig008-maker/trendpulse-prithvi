import pandas as pd
import matplotlib.pyplot as plt
import os

# -----------------------------------
# Load the cleaned CSV file
# -----------------------------------
file_path = "data/trends_clean.csv"

# Check if file exists
if not os.path.exists(file_path):
    print("Error: CSV file not found. Run Task 2 first.")
    exit()

# Read CSV
df = pd.read_csv(file_path)

# -----------------------------------
# Basic validation
# -----------------------------------
required_columns = ['category', 'score', 'num_comments']

for col in required_columns:
    if col not in df.columns:
        print(f"Error: Missing column '{col}' in CSV file.")
        exit()

# Remove missing values (clean safety)
df = df.dropna(subset=required_columns)

# -----------------------------------
# Create output folder
# -----------------------------------
output_folder = "outputs"

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# -----------------------------------
# 1. Number of posts per category
# -----------------------------------
category_counts = df['category'].value_counts().sort_values(ascending=False)

plt.figure()
category_counts.plot(kind='bar')
plt.title("Number of Posts per Category")
plt.xlabel("Category")
plt.ylabel("Number of Posts")
plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig(f"{output_folder}/posts_per_category.png")
plt.close()

# -----------------------------------
# 2. Average score per category
# -----------------------------------
avg_score = df.groupby('category')['score'].mean().sort_values(ascending=False)

plt.figure()
avg_score.plot(kind='bar')
plt.title("Average Score per Category")
plt.xlabel("Category")
plt.ylabel("Average Score")
plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig(f"{output_folder}/avg_score_per_category.png")
plt.close()

# -----------------------------------
# 3. Average comments per category
# -----------------------------------
avg_comments = df.groupby('category')['num_comments'].mean().sort_values(ascending=False)

plt.figure()
avg_comments.plot(kind='bar')
plt.title("Average Comments per Category")
plt.xlabel("Category")
plt.ylabel("Average Comments")
plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig(f"{output_folder}/avg_comments_per_category.png")
plt.close()

# -----------------------------------
# Final message
# -----------------------------------
print("Visualisations created successfully!")
print(f"Saved inside '{output_folder}/' folder")
