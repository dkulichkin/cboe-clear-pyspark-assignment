from pyspark.sql.window import Window
from pyspark.sql.functions import col, desc, first_value, row_number, lit, lag


def transform(df):
    w = Window.partitionBy('stock', 'date').orderBy(desc('id'))
    return df \
        .withColumn("recent_price_per_group", first_value(col("price")).over(w)) \
        .withColumn("recent_id_per_group", first_value(col("id")).over(w)) \
        .withColumn("price_group_row_number", row_number().over(w)) \
        .withColumn('updated_by', lag(col("id"), 1).over(w)) \
        .filter(
            (col("recent_price_per_group") != col("price")) |
            (col("price_group_row_number") == 1) |
            ((col("recent_price_per_group") == col("price")) & (col("updated_by") != col("recent_id_per_group")))
        ) \
        .withColumn("validated", lit(1)) \
        .select('id', 'stock', 'price', 'date', 'validated', 'updated_by') \
        .sort('id')