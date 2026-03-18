
import pyspark

from pyspark.sql import SparkSession
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/zoomcamp-project/keys/gcp-key.json"

spark = SparkSession.builder \
    .appName('GCS-Connect-Java17-Fix') \
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.14") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.environ["GOOGLE_APPLICATION_CREDENTIALS"]) \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED") \
    .getOrCreate()


def main():
    from pyspark.sql import functions as F

    tables = [
        "customers",
        "geolocation",
        "order_items",
        "payments",
        "reviews",
        "orders",
        "products",
        "sellers",
        "product_category_translation"
    ]

    base_input = "gs://ammar-zoomcamp-project/raw"
    base_output = "gs://ammar-zoomcamp-project/processed"

    for table in tables:
        try:
            print(f"🔄 Memproses {table}...")

            df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{base_input}/{table}.csv")

            df = df.dropna(how='all')

            for col_name, col_type in df.dtypes:
                if col_type == "string":
                    df = df.withColumn(col_name, F.trim(F.col(col_name)))


            df.write.mode("overwrite").parquet(f"{base_output}/{table}")

            print(f"✅ {table} Berhasil dikonversi!")

            del df

        except Exception as e:
            print(f"❌ Gagal memproses {table}: {str(e)}")

    print("\n🚀 SEMUA TABEL SELESAI. Anda bisa lanjut ke Kestra/BigQuery!")

if __name__ == "__main__":
    main()