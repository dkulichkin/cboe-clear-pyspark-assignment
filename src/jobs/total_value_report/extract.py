from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

schema = StructType([
    StructField("trade_id", StringType(), False),
    StructField("trade_report_type", StringType(), False),
    StructField("equity", StringType(), False),
    StructField("price", DecimalType(38, 2), False),  # DecimalType for safe money handling
    StructField("size", IntegerType(), False),
    StructField("side", StringType(), False),
    StructField("participant_id", StringType(), False)
])


def extract(spark, csv_path):
    return spark.read.csv(
        csv_path,
        header=True,
        mode="DROPMALFORMED",
        schema=schema
    )