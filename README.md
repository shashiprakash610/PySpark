# 🇩🇪 German Unemployment Analytics Platform
**A Spark-based ETL pipeline for processing official unemployment statistics from the Federal Employment Agency (BA Statistik)**

[![Spark](https://img.shields.io/badge/Apache_Spark-3.5.5-E25A1C)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-2.4.0-019733)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB)](https://python.org)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)

![Pipeline Architecture](docs/pipeline_architecture.png) <!-- Add actual diagram later -->

## 📌 Key Features
- **Multi-Dataset Integration**: Processes 6+ official BA Statistik datasets
- **Time-Series Optimizations**: Special handling for monthly data (2005-present)
- **Production-Grade Reliability**:
  - Data validation with Great Expectations
  - Delta Lake for ACID compliance
- **Regional Analysis**: East/West Germany comparisons
- **Economic Health Metrics**: Vacancy-to-unemployment ratios

## 📂 Dataset Inventory
| Dataset | Scope | Metrics | Update Freq | Sample Size |
|---------|-------|---------|------------|------------|
| [Unemployed Persons](data/raw/national/) | National | Total, Gender, Youth, Long-term | Monthly | 240+ records |
| [Unemployment Rates](data/raw/regional/) | Regional (States) | Seasonally adjusted rates | Quarterly | 16 states |
| [Economic Indicators](data/raw/economic/) | National | Vacancies, Short-time work | Weekly | 1000+ records |

## 🛠️ Technical Stack
| Component | Technology | Purpose |
|-----------|------------|---------|
| **Processing** | PySpark 3.5.5 | Distributed transformations |
| **Storage** | Delta Lake 2.4 | Time travel + versioning |
| **Orchestration** | Airflow 2.7 | Pipeline scheduling |
| **Monitoring** | Prometheus + Grafana | Performance tracking |
| **Testing** | PyTest + Great Expectations | Data quality |

## 🚀 Getting Started

### Prerequisites
```bash
# Java & Python
brew install openjdk@11 python@3.10

# Python environment
python -m venv .venv
source .venv/bin/activate