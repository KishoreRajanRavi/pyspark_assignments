import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from src.customer_purchase import utils


class CustomerPurchaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

        purchase_schema = StructType([
            StructField("customer", StringType(), True),
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

        cls.purchase_data_df = cls.spark.createDataFrame(purchase_data, schema=purchase_schema)
        cls.customer_products = utils.get_customer_products(cls.purchase_data_df)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_iphone13_only_buyers(self):
        df = utils.get_iphone13_only_buyers(self.customer_products)
        customers = [row['customer'] for row in df.collect()]
        self.assertIn('4', list(map(str, customers)))
        self.assertNotIn('1', list(map(str, customers)))  # because customer 1 bought many products

    def test_iphone14_upgrades(self):
        df = utils.get_iphone14_upgrades(self.customer_products)
        customers = [row['customer'] for row in df.collect()]
        self.assertIn(1, customers)
        self.assertIn(3, customers)
        self.assertNotIn(2, customers)


if __name__ == "__main__":
    unittest.main()
