# scripts/visualize.py
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pyarrow.parquet as pq
import matplotlib.ticker as ticker

# Configure Seaborn
sns.set(style="whitegrid")

# Path to processed Parquet files
PROCESSED_DIR = "Data/processed"

# Read all Parquet files into a single DataFrame
def load_all_parquet(path=PROCESSED_DIR):
    dataframes = []
    for file in os.listdir(path):
        if file.endswith(".parquet"):
            full_path = os.path.join(path, file)
            df = pd.read_parquet(full_path)
            dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True)

# Plot average unemployment count by year and region
def plot_unemployment_count(df):
    df = df[df["category"] == "Unemployment Count"]
    df = df[df["date"].notna() & df["total"].notna()]
    df["year"] = pd.to_datetime(df["date"]).dt.year
    grouped = df.groupby(["region", "year"])["total"].mean().reset_index()

    plt.figure(figsize=(12, 6))
    sns.lineplot(data=grouped, x="year", y="total", hue="region", marker="o")
    plt.title("Average Unemployment Count by Region (2005–2025)")
    plt.xlabel("Year")
    plt.ylabel("Unemployed Persons")
    plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    plt.tight_layout()
    plt.savefig("outputs/unemployment_trend.png")
    plt.show()

# Plot male vs female unemployment over time (totaled across Germany)
def plot_gender_trend(df):
    df = df[df["category"] == "Unemployment Count"]
    df = df[df["date"].notna() & df["male"].notna() & df["female"].notna()]
    df["year"] = pd.to_datetime(df["date"]).dt.year
    grouped = df.groupby("year")[["male", "female"]].mean().reset_index()

    plt.figure(figsize=(12, 6))
    sns.lineplot(data=grouped, x="year", y="male", label="Male", marker="o")
    sns.lineplot(data=grouped, x="year", y="female", label="Female", marker="o")
    plt.title("Unemployment Trend by Gender (Germany)")
    plt.xlabel("Year")
    plt.ylabel("Average Unemployed")
    plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    plt.tight_layout()
    plt.savefig("outputs/unemployment_gender_trend.png")
    plt.show()

# Plot youth unemployment vs long-term unemployment over time
def plot_youth_vs_longterm(df):
    df = df[df["category"] == "Unemployment Count"]
    df = df[df["date"].notna() & df["under_25"].notna() & df["long_term"].notna()]
    df["year"] = pd.to_datetime(df["date"]).dt.year
    grouped = df.groupby("year")[["under_25", "long_term"]].mean().reset_index()

    plt.figure(figsize=(12, 6))
    sns.lineplot(data=grouped, x="year", y="under_25", label="Youth (<25)", marker="o")
    sns.lineplot(data=grouped, x="year", y="long_term", label="Long-term", marker="o")
    plt.title("Youth vs Long-Term Unemployment (Germany)")
    plt.xlabel("Year")
    plt.ylabel("Average Count")
    plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    plt.tight_layout()
    plt.savefig("outputs/unemployment_youth_vs_longterm.png")
    plt.show()

# Plot seasonal unemployment (seasonally adjusted values if available)
def plot_seasonal_trend(df):
    df = df[df["category"] == "Short-time Workers"]
    df = df[df["date"].notna() & df["original_value"].notna()]
    df["month"] = pd.to_datetime(df["date"]).dt.month
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["original_value"] = pd.to_numeric(df["original_value"], errors="coerce")
    df = df[df["original_value"].notna()]

    grouped = df.groupby("month")["original_value"].mean().reset_index()

    plt.figure(figsize=(12, 6))
    sns.lineplot(data=grouped, x="month", y="original_value", marker="o")
    plt.title("Monthly Average Short-time Workers (Seasonality)")
    plt.xlabel("Month")
    plt.ylabel("Original Value (in 1000)")
    plt.xticks(range(1, 13))
    plt.tight_layout()
    plt.savefig("outputs/seasonal_monthly_trend.png")
    plt.show()

    print("🔍 Checking for seasonal data in 'Short-time Workers' category...")
    seasonal_df = df[df["category"] == "Short-time Workers"]
    print("Rows found:", len(seasonal_df))
    print("Columns:", seasonal_df.columns.tolist())
    print(seasonal_df.head(5))


if __name__ == "__main__":
    os.makedirs("outputs", exist_ok=True)
    df = load_all_parquet()
    plot_unemployment_count(df)
    plot_gender_trend(df)
    plot_youth_vs_longterm(df)
    plot_seasonal_trend(df)
