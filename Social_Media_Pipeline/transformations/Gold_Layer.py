import dlt
from pyspark.sql.functions import col, sum, avg, count, desc, to_date

# ====================================================
# 🟡 GOLD LAYER — BUSINESS AGGREGATIONS
# Reads from Silver Tables and Produces Analytical Views
# ====================================================

@dlt.table(
    name="gold_top_active_users",
    comment="Top 10 most active users based on total likes and comments on their posts."
)
def gold_top_active_users():
    # Read silver tables
    likes = dlt.read("silver_likes")
    comments = dlt.read("silver_comments")
    posts = dlt.read("silver_posts")

    # Count likes and comments per post
    likes_count = likes.groupBy("post_id").count().withColumnRenamed("count", "like_count")
    comments_count = comments.groupBy("post_id").count().withColumnRenamed("count", "comment_count")

    # Join posts with like and comment counts, aggregate engagement per user
    post_engagement = (
        posts.join(likes_count, "post_id", "left")
             .join(comments_count, "post_id", "left")
             .groupBy("user_id")
             .agg(
                 sum("like_count").alias("total_likes"),
                 sum("comment_count").alias("total_comments")
             )
             .fillna(0)
             .withColumn("engagement_score", col("total_likes") + col("total_comments"))
             .orderBy(desc("engagement_score"))
    )

    # Return top 10 most active users
    return post_engagement.limit(10)


@dlt.table(
    name="gold_popular_posts",
    comment="Top 10 most popular posts based on combined likes and comments."
)
def gold_popular_posts():
    # Read silver tables
    likes = dlt.read("silver_likes")
    comments = dlt.read("silver_comments")

    # Combine likes and comments counts per post
    engagement = (
        likes.groupBy("post_id").count().withColumnRenamed("count", "like_count")
            .join(
                comments.groupBy("post_id").count().withColumnRenamed("count", "comment_count"),
                "post_id",
                "outer"
            )
            .fillna(0)
            .withColumn("engagement_score", col("like_count") + col("comment_count"))
            .orderBy(desc("engagement_score"))
    )

    # Return top 10 most popular posts
    return engagement.limit(10)


@dlt.table(
    name="gold_daily_engagement_trends",
    comment="Daily engagement trends showing total likes and comments count over time."
)
def gold_daily_engagement_trends():
    # Read silver tables
    likes = dlt.read("silver_likes")
    comments = dlt.read("silver_comments")

    # Aggregate engagement counts by date
    likes_per_day = likes.groupBy(to_date("created_at").alias("date")).count().withColumnRenamed("count", "likes_count")
    comments_per_day = comments.groupBy(to_date("created_at").alias("date")).count().withColumnRenamed("count", "comments_count")

    # Join daily like and comment counts, compute total daily engagement
    daily_trends = (
        likes_per_day.join(comments_per_day, "date", "outer")
                     .fillna(0)
                     .withColumn("total_engagement", col("likes_count") + col("comments_count"))
                     .orderBy("date")
    )

    # Return daily engagement trends
    return daily_trends


@dlt.table(
    name="gold_user_engagement_summary",
    comment="User engagement summary showing average likes and comments per post."
)
def gold_user_engagement_summary():
    # Read silver tables
    posts = dlt.read("silver_posts")
    likes = dlt.read("silver_likes")
    comments = dlt.read("silver_comments")

    # Count likes and comments per post
    likes_per_post = likes.groupBy("post_id").count().withColumnRenamed("count", "like_count")
    comments_per_post = comments.groupBy("post_id").count().withColumnRenamed("count", "comment_count")

    # Join with posts and aggregate average engagement per user
    engagement = (
        posts.join(likes_per_post, "post_id", "left")
             .join(comments_per_post, "post_id", "left")
             .groupBy("user_id")
             .agg(
                 avg("like_count").alias("avg_likes_per_post"),
                 avg("comment_count").alias("avg_comments_per_post")
             )
             .fillna(0)
    )

    # Return user engagement summary
    return engagement