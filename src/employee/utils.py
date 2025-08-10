from pyspark.sql.functions import col, avg, current_date, lower

def create_employee_df(spark, employee_data, employee_schema):
    return spark.createDataFrame(employee_data, schema=employee_schema)

def create_department_df(spark, department_data, department_schema):
    return spark.createDataFrame(department_data, schema=department_schema)

def create_country_df(spark, country_data, country_schema):
    return spark.createDataFrame(country_data, schema=country_schema)

def average_salary_by_department(employee_df):
    return employee_df.groupBy("department").agg(avg("salary").alias("avg_salary"))

def employees_start_with_m(employee_df, department_df):
    join_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "inner")
    return join_df.filter(col("employee_name").startswith("m")) \
                  .select("employee_name", "dept_name")

def add_bonus_column(employee_df):
    return employee_df.withColumn("bonus", col("salary") * 2)

def reorder_employee_columns(employee_df):
    # Assuming bonus column exists
    return employee_df.select("employee_id", "employee_name", "salary", "state", "age", "department", "bonus")

def join_employee_department(employee_df, department_df, join_type="inner"):
    return employee_df.join(department_df, employee_df.department == department_df.dept_id, join_type)

def replace_state_with_country(employee_df, country_df):
    return employee_df.join(country_df, employee_df.state == country_df.country_code, "left") \
                      .drop("state", "country_code") \
                      .withColumnRenamed("country_name", "state")

def lowercase_columns_and_add_load_date(df):
    from pyspark.sql.functions import current_date
    lowercase_cols = [col(c).alias(c.lower()) for c in df.columns]
    return df.select(*lowercase_cols).withColumn("load_date", current_date())
