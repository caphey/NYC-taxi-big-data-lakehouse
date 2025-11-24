from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, sum, round, count

spark = SparkSession.builder \
    .appName("NYC Taxi ETL Job") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Démarrage du job ETL...")

# ==========================================
# BRONZE LAYER (Ingestion)
# ==========================================
print("Lecture des données brutes (Bronze Layer)...")
input_path = "/home/jovyan/data/bronze/yellow_tripdata_2023-01.parquet"
df_bronze = spark.read.parquet(input_path)

print(f"Données brutes chargées : {df_bronze.count()} lignes.")
# df_bronze.printSchema()

# ==========================================
# SILVER LAYER (Cleaning & Enrichment)
# ==========================================
print("Nettoyage et enrichissement des données (Silver Layer)...")
df_silver = df_bronze.filter((col("passenger_count") > 0) & (col("total_amount") > 0))

df_silver = df_silver.select(
    "tpep_pickup_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID", # Pick Up Location
    "DOLocationID", # Drop Off Location
    "payment_type",
    "total_amount",
    "tip_amount"
)

silver_path = "/home/jovyan/data/silver/taxi_clean"
df_silver.write.mode("overwrite").parquet(silver_path)

print(f"Données nettoyées sauvegardées dans Silver ({df_silver.count()} lignes).")

# ==========================================
# GOLD LAYER (Business Aggregations)
# ==========================================
print("Calcul des agrégations métier (Gold Layer)...")

# Analyse 1 : Quels sont les endroits qui rapportent le plus de pourboires ?
df_gold_tips = df_silver.groupBy("PULocationID") \
    .agg(
        avg("tip_amount").alias("avg_tip"),
        count("*").alias("total_trips")
    ) \
    .where(col("total_trips") > 50) \
    .orderBy(col("avg_tip").desc())

# Analyse 2 : Revenue total par nombre de passagers 
df_gold_revenue = df_silver.groupBy("passenger_count") \
    .agg(
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_ticket")
    ) \
    .orderBy("passenger_count")

df_gold_tips.write.mode("overwrite").parquet("/home/jovyan/data/gold/top_tips_zones")
df_gold_revenue.write.mode("overwrite").parquet("/home/jovyan/data/gold/revenue_stats")

print("\nTop Zones pour les pourboires :")
df_gold_tips.show(5)

print("\nRevenus par nombre de passagers :")
df_gold_revenue.show()

spark.stop()