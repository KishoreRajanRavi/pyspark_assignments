import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.employee import  utils

class EmployeeUtilsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("test_employee_utils").getOrCreate()

        cls.employee_schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("employee_name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("state", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("age", IntegerType(), True)
        ])
        cls.employee_data = [
            (11, "james", "D101", "ny", 9000, 34),
            (12, "michel", "D101", "ny", 8900, 32),
            (17, "maria", "D101", "ny", 7900, 40)
        ]
        cls.department_schema = StructType([
            StructField("dept_id", StringType(), True),
            StructField("dept_name", StringType(), True)
        ])
        cls.department_data = [
            ("D101", "sales"),
            ("D102", "finance"),
        ]
        cls.country_schema = StructType([
            StructField("country_code", StringType(), True),
            StructField("country_name", StringType(), True)
        ])
        cls.country_data = [
            ("ny", "newyork"),
            ("ca", "California"),
        ]

        cls.employee_df = utils.create_employee_df(cls.spark, cls.employee_data, cls.employee_schema)
        cls.department_df = utils.create_department_df(cls.spark, cls.department_data, cls.department_schema)
        cls.country_df = utils.create_country_df(cls.spark, cls.country_data, cls.country_schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_average_salary_by_department(self):
        result_df = utils.average_salary_by_department(self.employee_df)
        result = {row['department']: row['avg_salary'] for row in result_df.collect()}
        self.assertAlmostEqual(result["D101"], (9000 + 8900 + 7900) / 3)

    def test_employees_start_with_m(self):
        result_df = utils.employees_start_with_m(self.employee_df, self.department_df)
        names = [row["employee_name"] for row in result_df.collect()]
        self.assertIn("maria", names)
        self.assertNotIn("james", names)

    def test_add_bonus_column(self):
        df_with_bonus = utils.add_bonus_column(self.employee_df)
        rows = df_with_bonus.collect()
        for row in rows:
            self.assertEqual(row["bonus"], row["salary"] * 2)

    def test_replace_state_with_country(self):
        df_with_country = utils.replace_state_with_country(self.employee_df, self.country_df)
        states = [row["state"] for row in df_with_country.collect()]
        self.assertIn("newyork", states)

    def test_lowercase_columns_and_add_load_date(self):
        df_with_country = utils.replace_state_with_country(self.employee_df, self.country_df)
        final_df = utils.lowercase_columns_and_add_load_date(df_with_country)
        columns = final_df.columns
        self.assertIn("load_date", columns)
        self.assertTrue(all(c.islower() for c in columns if c != "load_date"))

if __name__ == "__main__":
    unittest.main()
