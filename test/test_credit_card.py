import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from src.credit_card import utils

class CreditCardTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("test_credit_card").getOrCreate()

        schema = StructType([StructField("card_number", StringType(), True)])
        data = [
            ("1234567891234567",),
            ("5678912345671234",),
            (None,),
            ("0000",)
        ]
        cls.df = cls.spark.createDataFrame(data, schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_mask_card_number(self):
        masked_df = utils.add_masked_card_column(self.df)
        results = masked_df.select("card_number", "masked_card_number").collect()

        for row in results:
            card = row["card_number"]
            masked = row["masked_card_number"]
            if card is None:
                self.assertIsNone(masked)
            else:
                self.assertTrue(masked.endswith(card[-4:]))
                self.assertEqual(len(masked), len(card))
                self.assertTrue(all(c == "*" for c in masked[:-4]))

    def test_partitions(self):
        orig_partitions = self.df.rdd.getNumPartitions()  # e.g. 8

        repartitioned = self.df.repartition(3)
        self.assertEqual(repartitioned.rdd.getNumPartitions(), 3)

        coalesced = repartitioned.coalesce(orig_partitions)

        # Since coalesce does NOT increase partitions, partitions remain 3 here
        self.assertEqual(coalesced.rdd.getNumPartitions(), 3)


if __name__ == "__main__":
    unittest.main()
