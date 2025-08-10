from pyspark.sql.functions import collect_set, size, array_contains

def get_customer_products(purchase_data_df):
    return purchase_data_df.groupby("customer") \
        .agg(collect_set("product_model").alias("products"))

def get_iphone13_only_buyers(customer_products):
    return customer_products \
        .filter(size("products") == 1) \
        .filter(array_contains("products", "iphone13"))

def get_iphone14_upgrades(customer_products):
    return customer_products \
        .filter(array_contains("products", "iphone13") & array_contains("products", "iphone14"))

def get_customers_bought_all_products(customer_products, total_products_count):
    return customer_products.filter(size("products") == total_products_count)
