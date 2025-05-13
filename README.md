# 🇩🇪 German Unemployment Analytics Pipeline

**A PySpark-based ETL pipeline for processing official unemployment statistics from the Federal Employment Agency (BA Statistik)**

[![Spark](https://img.shields.io/badge/Apache_Spark-3.5.5-E25A1C)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.12%2B-3776AB)](https://python.org)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)

---

## 📌 Key Features

- 🔄 **ETL Pipeline with PySpark**
  - Reads raw `.csv` files from BA Statistik (2005–2025)
  - Skips malformed rows, standardizes structure, adds metadata
- 🧱 **Schema-Driven Design**
  - Explicit Spark schemas for each dataset category
- 📊 **Time-Series Ready**
  - Parses dates, extracts `year` and `month`, and aggregates over time
- 📦 **Outputs to Parquet**
  - Clean data is saved in optimized columnar format

---

## 📂 Dataset Categories

| Category             | Examples                                | Scope                  |
|----------------------|-----------------------------------------|------------------------|
| Unemployment Count   | `arbeitslose_deutschland_originalwert.csv` | Germany, East, West    |
| Unemployment Rate    | `arbeitslosenquote_*.csv`               | Germany, East, West    |
| Seasonally Adjusted  | `kurzarbeiter_bv41.csv`, `stellen.csv` | National Level         |

---

## 🛠️ Tech Stack

| Layer        | Tool           |
|--------------|----------------|
| Processing   | PySpark        |
| Storage      | Parquet Files  |
| Scripting    | Python 3.12    |
| IDE Tested On| VSCode + MacOS |

---

## 🚀 Getting Started

### 🔧 Prerequisites

```bash
# Install Python & Java
brew install python openjdk@11

# Set up virtual environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
