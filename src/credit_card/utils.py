from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def mask_card_number(card):
    if card is None:
        return None
    return "*" * (len(card) - 4) + card[-4:]

mask_card_udf = udf(mask_card_number, StringType())

def add_masked_card_column(df, card_col="card_number"):
    return df.withColumn("masked_card_number", mask_card_udf(col(card_col)))
