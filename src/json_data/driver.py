from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, year, month, dayofmonth
import utils

def main():
    spark = SparkSession.builder.appName("json_data_processing").getOrCreate()

    json_path = r"C:\Users\KishoreRajanRavi\Desktop\Pyspark_Ass\data\json\nested_json_file.json"
    df = spark.read.option("multiline", "true").json(json_path)

    print("Original schema:")
    df.printSchema()
    print("Original data:")
    df.show(truncate=False)

    df_flat = utils.flatten_properties(df)
    print("Flattened properties:")
    df_flat.show(truncate=False)

    df_exploded = utils.explode_employees(df_flat)
    print("Exploded employees:")
    df_exploded.show(truncate=False)

    df_filtered = utils.filter_by_id(df_exploded, 1001)
    print("Filtered rows where id=1001:")
    df_filtered.show(truncate=False)

    df_final = df_filtered.withColumn("load_date", current_date()) \
        .withColumn("year", year("load_date")) \
        .withColumn("month", month("load_date")) \
        .withColumn("day", dayofmonth("load_date"))

    print("Final output with date columns:")
    df_final.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
