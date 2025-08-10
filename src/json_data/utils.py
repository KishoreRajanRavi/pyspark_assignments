from pyspark.sql.functions import col, explode

def flatten_properties(df):
    """Flatten 'properties' struct into columns"""
    return df.select(
        col("id"),
        col("properties.name").alias("name"),
        col("properties.storeSize").alias("store_size"),
        col("employees")
    )

def explode_employees(df_flat):
    """Explode 'employees' array into separate rows and select employee fields"""
    df_exploded = df_flat.select(
        "id", "name", "store_size",
        explode("employees").alias("employee")
    )
    return df_exploded.select(
        "id", "name", "store_size",
        col("employee.empId").alias("emp_id"),
        col("employee.empName").alias("emp_name")
    )

def filter_by_id(df, filter_id):
    """Filter dataframe by id"""
    return df.filter(col("id") == filter_id)
