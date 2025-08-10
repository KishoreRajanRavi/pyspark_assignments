from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import utils

def main():
    spark = SparkSession.builder.appName("CreditCardAssignment").getOrCreate()

    schema = StructType([StructField("card_number", StringType(), True)])

    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]

    credit_card_df = spark.createDataFrame(data, schema)

    print("\nOriginal DataFrame:")
    credit_card_df.show(truncate=False)

    original_partitions = credit_card_df.rdd.getNumPartitions()
    print(f"\nOriginal number of partitions: {original_partitions}")

    credit_card_df_5 = credit_card_df.repartition(5)
    print(f"Number of partitions after increase: {credit_card_df_5.rdd.getNumPartitions()}")

    credit_card_df_original = credit_card_df_5.coalesce(original_partitions)
    print(f"Number of partitions after decreasing: {credit_card_df_original.rdd.getNumPartitions()}")

    masked_df = utils.add_masked_card_column(credit_card_df)
    print("\nFinal Output with masked card numbers:")
    masked_df.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
