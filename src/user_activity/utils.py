from pyspark.sql.functions import col, to_timestamp, to_date, current_timestamp, date_sub, count

def rename_columns(df):
    return df.withColumnRenamed("log id", "log_id") \
             .withColumnRenamed("user$id", "user_id") \
             .withColumnRenamed("action", "user_activity") \
             .withColumnRenamed("timestamp", "time_stamp")

def convert_timestamp(df):
    return df.withColumn("time_stamp", to_timestamp(col("time_stamp"), "yyyy-MM-dd HH:mm:ss"))

def add_login_date(df):
    return df.withColumn("login_date", to_date(col("time_stamp")))



def actions_in_last_n_days(df, days=7, current_date_col=None):
    # Use provided current_date_col or default to current timestamp
    if current_date_col is None:
        current_date_col = current_timestamp()
    cutoff = date_sub(current_date_col, days)
    return df.filter(col("time_stamp") >= cutoff) \
             .groupBy("user_id") \
             .agg(count("user_activity").alias(f"actions_in_last_{days}_days"))
