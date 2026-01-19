import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import TimestampType

# ===============================
# Base paths
# ===============================
BASE_INPUT = "/Volumes/kusha_solutions/jeevan_streaming/my_volume/Raw_JSON_Files/"  # Input directory for raw JSON files
SCHEMA_LOCATION_BASE = "/Volumes/kusha_solutions/jeevan_streaming/my_volume/autoloader_schema/"  # Directory for autoloader schema checkpoints

# ===============================
# Schemas for each source
# ===============================
# Schema for user data
user_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("username", StringType(), True),
    StructField("email", StringType(), True),
    StructField("location", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Schema for post data
post_schema = StructType([
    StructField("post_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("content", StringType(), True),
    StructField("likes", IntegerType(), True),
    StructField("created_at", TimestampType(), True)
])

# Schema for comment data
comment_schema = StructType([
    StructField("comment_id", IntegerType(), True),
    StructField("post_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("comment", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Schema for like data
like_schema = StructType([
    StructField("like_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("post_id", IntegerType(), True),
    StructField("reaction", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# ===============================
# Bronze tables
# ===============================

# Bronze table for raw user data stream
@dlt.table(
    name="bronze_users",
    comment="Bronze: Raw user data stream with metadata"
)
def bronze_users():
    return (
        spark.readStream
            .format("cloudFiles")  # Use Autoloader for streaming ingestion
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/users")  # Autoloader schema checkpoint
            .option("multiline", "true")
            .schema(user_schema)  # Apply user schema
            .load(f"{BASE_INPUT}/users/")  # Load user data files
            .withColumn("ingest_time", current_timestamp())  # Add ingestion timestamp
            .withColumn("source_file", col("_metadata.file_path"))  # Add source file path metadata
    )

# Bronze table for raw post data stream
@dlt.table(
    name="bronze_posts",
    comment="Bronze: Raw post data stream with metadata"
)
def bronze_posts():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/posts")
            .option("multiline", "true")
            .schema(post_schema)
            .load(f"{BASE_INPUT}/posts/")
            .withColumn("ingest_time", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
    )

# Bronze table for raw comment data stream
@dlt.table(
    name="bronze_comments",
    comment="Bronze: Raw comment data stream with metadata"
)
def bronze_comments():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/comments")
            .option("multiline", "true")
            .schema(comment_schema)
            .load(f"{BASE_INPUT}/comments/")
            .withColumn("ingest_time", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
    )

# Bronze table for raw like data stream
@dlt.table(
    name="bronze_likes",
    comment="Bronze: Raw like data stream with metadata"
)
def bronze_likes():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/likes")
            .option("multiline", "true")
            .schema(like_schema)
            .load(f"{BASE_INPUT}/likes/")
            .withColumn("ingest_time", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
    )