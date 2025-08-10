from pyspark.sql import SparkSession
import  utils

def main():
    spark = SparkSession.builder.appName("UserActivitySimple").enableHiveSupport().getOrCreate()

    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')
    ]

    schema = "log id int, user$id int, action string, timestamp string"

    df = spark.createDataFrame(data, schema)

    df = utils.rename_columns(df)
    df = utils.convert_timestamp(df)

    print("Original DataFrame:")
    df.show(truncate=False)

    actions_df = utils.actions_in_last_n_days(df, 7)
    print("Actions in last 7 days:")
    actions_df.show()

    df = utils.add_login_date(df)
    print("DataFrame with login_date:")
    df.show(truncate=False)

    output_path = "/tmp/user_activity_csv_simple"
    df.write.mode("overwrite").option("header", True).csv(output_path)
    print(f"Data saved as CSV at: {output_path}")

    spark.sql("CREATE DATABASE IF NOT EXISTS user")
    df.write.mode("overwrite").saveAsTable("user.login_details")
    print("Managed table user.login_details created successfully.")

    spark.stop()

if __name__ == "__main__":
    main()
