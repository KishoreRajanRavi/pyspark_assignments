import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from src.json_data import utils

class JsonDataTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("json_data_test").getOrCreate()

        employee_struct = StructType([
            StructField("empId", IntegerType(), True),
            StructField("empName", StringType(), True),
        ])

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("properties", StructType([
                StructField("name", StringType(), True),
                StructField("storeSize", StringType(), True),
            ])),
            StructField("employees", ArrayType(employee_struct), True),
        ])

        cls.data = [
            (1001, {"name": "StoreA", "storeSize": "Large"}, [{"empId": 1, "empName": "Alice"}, {"empId": 2, "empName": "Bob"}]),
            (1002, {"name": "StoreB", "storeSize": "Medium"}, [{"empId": 3, "empName": "Charlie"}]),
        ]

        cls.df = cls.spark.createDataFrame(cls.data, schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_flatten_properties(self):
        df_flat = utils.flatten_properties(self.df)
        cols = df_flat.columns
        self.assertIn("id", cols)
        self.assertIn("name", cols)
        self.assertIn("store_size", cols)
        self.assertIn("employees", cols)
        self.assertEqual(df_flat.count(), 2)

    def test_explode_employees(self):
        df_flat = utils.flatten_properties(self.df)
        df_exploded = utils.explode_employees(df_flat)
        cols = df_exploded.columns
        self.assertIn("emp_id", cols)
        self.assertIn("emp_name", cols)
        self.assertEqual(df_exploded.count(), 3)  # 2 + 1 employees total

    def test_filter_by_id(self):
        df_flat = utils.flatten_properties(self.df)
        df_exploded = utils.explode_employees(df_flat)
        df_filtered = utils.filter_by_id(df_exploded, 1001)
        ids = [row["id"] for row in df_filtered.collect()]
        self.assertTrue(all(i == 1001 for i in ids))
        self.assertEqual(df_filtered.count(), 2)

if __name__ == "__main__":
    unittest.main()
