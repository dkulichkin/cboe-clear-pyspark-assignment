from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DateType, ByteType

schema = StructType([
    StructField("id", LongType(), False),
    StructField("stock", StringType(), False),
    StructField("price", DecimalType(38, 5), False),
    StructField("date", DateType(), False),
    StructField("validated", ByteType(), False),
    StructField("updated_by", LongType(), True)
])


def extract(spark, csv_path):
    return spark.read.csv(
        csv_path,
        header=True,
        mode="DROPMALFORMED",
        schema=schema
    )