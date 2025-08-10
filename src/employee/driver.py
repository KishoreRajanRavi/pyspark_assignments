from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import utils

def main():
    spark = SparkSession.builder.appName("EmployeeDataApp").getOrCreate()

    # Schemas and data (as per your example)
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("state", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("age", IntegerType(), True)
    ])
    employee_data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]

    department_schema = StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True)
    ])
    department_data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    country_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])
    country_data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]

    # Create DataFrames
    employee_df = utils.create_employee_df(spark, employee_data, employee_schema)
    department_df = utils.create_department_df(spark, department_data, department_schema)
    country_df = utils.create_country_df(spark, country_data, country_schema)

    # Call utils functions and show results (you can customize prints)
    avg_salary_df = utils.average_salary_by_department(employee_df)
    avg_salary_df.show()

    m_employees_df = utils.employees_start_with_m(employee_df, department_df)
    m_employees_df.show()

    employee_df = utils.add_bonus_column(employee_df)
    employee_df = utils.reorder_employee_columns(employee_df)

    inner_join_df = utils.join_employee_department(employee_df, department_df, "inner")
    print("Inner Join:")
    inner_join_df.show()

    employee_with_country_df = utils.replace_state_with_country(employee_df, country_df)
    final_df = utils.lowercase_columns_and_add_load_date(employee_with_country_df)

    final_df.show()

    # Save output if needed
    # final_df.write.mode("overwrite").parquet("path_to_parquet")
    # final_df.write.mode("overwrite").option("header", True).csv("path_to_csv")

    spark.stop()

if __name__ == "__main__":
    main()
