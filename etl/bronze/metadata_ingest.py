from pyspark.sql import SparkSession

POSTGRES_URL = "jdbc:postgresql://localhost:5432/hospital_db"
POSTGRES_PROPERTIES = {
    "user": "hospital",
    "password": "hospital123",
    "driver": "org.postgresql.Driver",
}

def create_spark_session():
    return (
        SparkSession.builder
        .appName("BronzeMetadataIngest")
        .config("spark.jars", "jars/postgresql-42.7.3.jar,jars/delta-spark_4.1_2.13-4.1.0.jar,jars/delta-storage-4.1.0.jar")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def ingest_to_bronze():
    spark = create_spark_session()

    df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="raw_class_info",
        properties=POSTGRES_PROPERTIES,
    )

    print(f"Read {df.count()} rows from Postgres")
    df.printSchema()

    df.write.format("delta").mode("overwrite").save("bronze/metadata_raw")

    print("Wrote to bronze/metadata_raw as Delta table")
    spark.stop()

if __name__ == "__main__":
    ingest_to_bronze()