from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
import utils

def main():
    spark = SparkSession.builder.appName("customer_purchase").getOrCreate()

    purchase_schema = StructType([
        StructField("customer", StringType(), True),
        StructField("product_model", StringType(), True)
    ])

    product_schema = StructType([
        StructField("product_model", StringType(), True)
    ])

    purchase_data = [
        (1, "iphone13"),
        (1, "dell i5 core"),
        (2, "iphone13"),
        (2, "dell i5 core"),
        (3, "iphone13"),
        (3, "dell i5 core"),
        (1, "dell i3 core"),
        (1, "hp i5 core"),
        (1, "iphone14"),
        (3, "iphone14"),
        (4, "iphone13")
    ]

    product_data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)
    ]

    purchase_data_df = spark.createDataFrame(purchase_data, schema=purchase_schema)
    product_data_df = spark.createDataFrame(product_data, schema=product_schema)

    customer_products = utils.get_customer_products(purchase_data_df)

    iphone13_only_buyers = utils.get_iphone13_only_buyers(customer_products)
    print("Customers who bought only iphone13:")
    iphone13_only_buyers.show(truncate=False)

    iphone14_upgrades = utils.get_iphone14_upgrades(customer_products)
    print("Customers who upgraded from iphone13 to iphone14:")
    iphone14_upgrades.show(truncate=False)

    total_products = product_data_df.distinct().count()
    customers_bought_all = utils.get_customers_bought_all_products(customer_products, total_products)
    print("Customers who bought all models:")
    customers_bought_all.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
