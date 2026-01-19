import dlt
from pyspark.sql.functions import col, trim, lower, current_timestamp

# ============================
# Silver Layer: Users
# ============================
# This table processes raw user data from the bronze layer.
# Steps:
#   - Removes duplicate users based on user_id.
#   - Fills missing values for key columns.
#   - Standardizes text fields (trims whitespace, lowercases username).
#   - Adds a processed timestamp for auditing.
#   - Applies data quality checks: user_id, username, and email must be present and valid.
@dlt.table(
    name="silver_users",
    comment="Silver: Cleaned and standardized user data"
)
@dlt.expect_all_or_drop({
    "non_null_user_id": "user_id IS NOT NULL",
    "non_null_username": "username IS NOT NULL",
    "valid_email": "email LIKE '%@%'"
})
def silver_users():
    df = dlt.read("bronze_users")
    
    # Remove duplicates
    df = df.dropDuplicates(["user_id"])
    
    # Handle nulls
    df = df.fillna({
        "name": "Unknown",
        "username": "unknown",
        "email": "unknown@example.com",
        "location": "Unknown"
    })
    
    # Standardize text columns
    df = df.withColumn("username", lower(trim(col("username")))) \
           .withColumn("name", trim(col("name"))) \
           .withColumn("location", trim(col("location")))
    
    # Add processed timestamp
    df = df.withColumn("processed_time", current_timestamp())
    
    return df


# ============================
# Silver Layer: Posts
# ============================
# This table processes raw post data from the bronze layer.
# Steps:
#   - Removes duplicate posts based on post_id.
#   - Fills missing values for content and likes.
#   - Trims whitespace from content.
#   - Adds a processed timestamp for auditing.
#   - Applies data quality checks: post_id, user_id, and content must be present.
@dlt.table(
    name="silver_posts",
    comment="Silver: Cleaned and standardized post data"
)
@dlt.expect_all_or_drop({
    "non_null_post_id": "post_id IS NOT NULL",
    "non_null_user_id": "user_id IS NOT NULL",
    "non_null_content": "content IS NOT NULL"
})
def silver_posts():
    df = dlt.read("bronze_posts")
    
    # Remove duplicates
    df = df.dropDuplicates(["post_id"])
    
    # Handle nulls
    df = df.fillna({
        "content": "No content",
        "likes": 0
    })
    
    # Standardize text columns
    df = df.withColumn("content", trim(col("content")))
    
    # Add processed timestamp
    df = df.withColumn("processed_time", current_timestamp())
    
    return df


# ============================
# Silver Layer: Comments
# ============================
# This table processes raw comment data from the bronze layer.
# Steps:
#   - Removes duplicate comments based on comment_id.
#   - Fills missing values for comment text.
#   - Trims whitespace from comment text.
#   - Adds a processed timestamp for auditing.
#   - Applies data quality checks: comment_id, post_id, user_id, and comment must be present.
@dlt.table(
    name="silver_comments",
    comment="Silver: Cleaned and standardized comment data"
)
@dlt.expect_all_or_drop({
    "non_null_comment_id": "comment_id IS NOT NULL",
    "non_null_post_id": "post_id IS NOT NULL",
    "non_null_user_id": "user_id IS NOT NULL",
    "non_null_comment": "comment IS NOT NULL"
})
def silver_comments():
    df = dlt.read("bronze_comments")
    
    # Remove duplicates
    df = df.dropDuplicates(["comment_id"])
    
    # Handle nulls
    df = df.fillna({"comment": "No comment"})
    
    # Standardize text columns
    df = df.withColumn("comment", trim(col("comment")))
    
    # Add processed timestamp
    df = df.withColumn("processed_time", current_timestamp())
    
    return df


# ============================
# Silver Layer: Likes
# ============================
# This table processes raw like data from the bronze layer.
# Steps:
#   - Removes duplicate likes based on like_id.
#   - Fills missing values for reaction type.
#   - Standardizes reaction text (trims whitespace, lowercases).
#   - Adds a processed timestamp for auditing.
#   - Applies data quality checks: like_id, post_id, and user_id must be present.
@dlt.table(
    name="silver_likes",
    comment="Silver: Cleaned and standardized like data"
)
@dlt.expect_all_or_drop({
    "non_null_like_id": "like_id IS NOT NULL",
    "non_null_post_id": "post_id IS NOT NULL",
    "non_null_user_id": "user_id IS NOT NULL"
})
def silver_likes():
    df = dlt.read("bronze_likes")
    
    # Remove duplicates
    df = df.dropDuplicates(["like_id"])
    
    # Handle nulls
    df = df.fillna({"reaction": "like"})
    
    # Standardize text columns
    df = df.withColumn("reaction", lower(trim(col("reaction"))))
    
    # Add processed timestamp
    df = df.withColumn("processed_time", current_timestamp())
    
    return df