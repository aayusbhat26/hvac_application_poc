import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session(app_name):
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

    s3_access_key = os.environ.get("HF_S3_ACCESS_KEY")
    s3_secret_key = os.environ.get("HF_S3_SECRET_KEY")
    
    extra_packages = []
    
    if s3_access_key and s3_secret_key:
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", "https://s3.hf.co/aayushbhat26") \
            .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        extra_packages.append("org.apache.hadoop:hadoop-aws:3.3.4")
        extra_packages.append("com.amazonaws:aws-java-sdk-bundle:1.12.262")

    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

def ensure_dir(path):
    if not path.startswith("s3a://") and not path.startswith("s3://"):
        os.makedirs(path, exist_ok=True)

def path_exists(spark, path):
    """Check if a path exists. Works for both local and S3A paths."""
    if path.startswith("s3a://") or path.startswith("s3://"):
        sc = spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI(path), 
            sc._jsc.hadoopConfiguration()
        )
        return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))
    else:
        return os.path.exists(path)
