# scripts/load.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import SparkSession
from config.schemas.schemas import STRUCTURAL_SCHEMA, RATE_SCHEMA, ADJUSTED_SCHEMA

# Add root path to sys.path for proper module imports



from scripts.transform import process_dataset, select_schema

def preprocess_csv_skip_first_line(input_path, output_path):
    with open(input_path, "r", encoding="latin1") as infile:
        lines = infile.readlines()

    if not lines:
        return

    header = lines[0] \
        .replace("Datum", "date") \
        .replace("Originalwert, in 1000", "original_value") \
        .replace("Originalwert, VerÃ¤nderung gegenÃ¼ber Vorjahresmonat in %", "yoy_change_percent") \
        .replace("Originalwert, in 1000, VerÃ¤nderung gegenÃ¼ber Vorjahresmonat absolut", "yoy_change_absolute") \
        .replace("Trend-Konjunktur-Komponente (BV 4.1), in 1000", "trend_value") \
        .replace("Trend-Konjunktur-Komponente (BV 4.1), VerÃ¤nderung gegenÃ¼ber Vormonat in %", "trend_monthly_change") \
        .replace("Kalender- und saisonbereinigte Werte (BV 4.1), in 1000", "seasonally_adjusted") \
        .replace("Kalender- und saisonbereinigte Werte (BV 4.1), VerÃ¤nderung gegenÃ¼ber Vormonat in %", "sa_monthly_change") \
        .replace("Restkomponente (BV 4.1)", "residual_component")

    lines[0] = header

    with open(output_path, "w", encoding="utf-8") as outfile:
        outfile.writelines(lines[1:])  # Skip title row



def load_all_data():
    spark = SparkSession.builder \
        .appName("BA Statistik ETL") \
        .config("spark.sql.shuffle.partitions", 4) \
        .getOrCreate()

    raw_dir = "Data/Raw"
    processed_dir = "Data/processed"
    temp_dir = "Data/tmp"

    os.makedirs(temp_dir, exist_ok=True)

    for root, _, files in os.walk(raw_dir):
        for file in files:
            if not file.endswith(".csv"):
                continue

            full_path = os.path.join(root, file)
            print(f"Processing {file}...")

            # Step 1: Skip the first line and write to temp
            temp_path = os.path.join(temp_dir, file)
            preprocess_csv_skip_first_line(full_path, temp_path)

            # Step 2: Pick schema based on filename
            schema = select_schema(file)
            if not schema:
                print(f"⚠️ Skipping {file}: unknown schema")
                continue

            df = spark.read \
                .option("header", True) \
                .option("delimiter", ";") \
                .option("encoding", "utf-8") \
                .schema(schema) \
                .csv(temp_path)

            df = df.replace(".", None)

            if len(df.columns) < 2:
                print(f"⚠️ Skipping {file}: not enough columns")
                continue
            print("DEBUG FILE:", file)  # make sure this is only the filename, not full path

            # Step 3: Run transformation
            transformed_df = process_dataset(df, file)

            # Step 4: Save
            output_path = os.path.join(processed_dir, file.replace(".csv", ".parquet"))
            transformed_df.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    load_all_data()
