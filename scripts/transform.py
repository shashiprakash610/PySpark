# scripts/transform.py
from pyspark.sql.functions import col, lit, to_date, regexp_replace
from config.schemas.schemas import STRUCTURAL_SCHEMA, RATE_SCHEMA, ADJUSTED_SCHEMA

# Determine region from file name
def get_region(file_name: str):
    name = file_name.lower()
    if "frueheres" in name:
        return "Former West Germany"
    elif "neue" in name:
        return "New States"
    elif "deutschland" in name:
        return "Germany"
    else:
        return "Unknown"

# Assign category based on file name
def get_category(file_name: str):
    name = file_name.lower()
    if "kurzarbeiter" in name:
        return "Short-time Workers"
    elif "quote" in name:
        return "Unemployment Rate"
    elif "arbeitslose" in name:
        return "Unemployment Count"
    elif "stellen" in name:
        return "Job Vacancies"
    else:
        return "Other"

# Pick schema based on filename
def select_schema(file_name: str):
    name = file_name.lower()
    if "kurzarbeiter" in name or "stellen" in name:
        return ADJUSTED_SCHEMA
    elif "quote" in name:
        return RATE_SCHEMA
    elif "arbeitslose" in name:
        return STRUCTURAL_SCHEMA
    else:
        return None

# Process and annotate the DataFrame
def process_dataset(df, file_name: str):
    region = get_region(file_name)
    category = get_category(file_name)

    # Fix date parsing and attach metadata
    df = df.withColumn("date", regexp_replace("date", "\\.", "/"))
    df = df.withColumn("date", to_date(col("date"), "dd/MM/yyyy"))
    df = df.withColumn("region", lit(region))
    df = df.withColumn("category", lit(category))
    df = df.withColumn("source_file", lit(file_name))

    return df
