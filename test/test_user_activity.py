import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col
from src.user_activity import  utils
from pyspark.sql.functions import lit, to_timestamp
class UserActivityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("UserActivityTest").getOrCreate()

        schema = StructType([
            StructField("log id", IntegerType(), True),
            StructField("user$id", IntegerType(), True),
            StructField("action", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])

        cls.data = [
            (1, 101, 'login', '2023-09-05 08:30:00'),
            (2, 102, 'click', '2023-09-06 12:45:00'),
            (3, 101, 'click', '2023-09-07 14:15:00'),
            (4, 103, 'login', '2023-09-08 09:00:00'),
            (5, 102, 'logout', '2023-09-09 17:30:00'),
            (6, 101, 'click', '2023-09-10 11:20:00'),
            (7, 103, 'click', '2023-09-11 10:15:00'),
            (8, 102, 'click', '2023-09-12 13:10:00')
        ]

        cls.df = cls.spark.createDataFrame(cls.data, schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_rename_columns(self):
        df_renamed = utils.rename_columns(self.df)
        expected_cols = {"log_id", "user_id", "user_activity", "time_stamp"}
        self.assertTrue(set(df_renamed.columns).issuperset(expected_cols))

    def test_convert_timestamp(self):
        df_renamed = utils.rename_columns(self.df)
        df_ts = utils.convert_timestamp(df_renamed)
        self.assertEqual(df_ts.schema["time_stamp"].dataType.typeName(), "timestamp")

    def test_add_login_date(self):
        df_renamed = utils.rename_columns(self.df)
        df_ts = utils.convert_timestamp(df_renamed)
        df_with_date = utils.add_login_date(df_ts)
        self.assertIn("login_date", df_with_date.columns)



    def test_actions_in_last_n_days(self):
        df_renamed = utils.rename_columns(self.df)
        df_ts = utils.convert_timestamp(df_renamed)

        # Fixed current date to match test data range
        fixed_now = to_timestamp(lit("2023-09-15 00:00:00"))

        actions_df = utils.actions_in_last_n_days(df_ts, days=10, current_date_col=fixed_now)

        self.assertIn("actions_in_last_10_days", actions_df.columns)
        counts = actions_df.collect()
        self.assertTrue(len(counts) > 0)


if __name__ == "__main__":
    unittest.main()
