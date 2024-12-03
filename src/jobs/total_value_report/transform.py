from decimal import Decimal
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from pyspark.sql.functions import \
    row_number, desc, monotonically_increasing_id, \
    col, count, collect_list, array, lit, udf, struct


def populate_row_ids(df):
    """
        we need some ids for ordering within window partitions so having nothing else
        relying on a synthetic id

        Attention: this will not with multiple CSV files

        :param df: DataFrame
        :return: DataFrame
        """
    return df.withColumn("row_id", monotonically_increasing_id())


def slice_last_trade_states_per_party(df):
    """
        to keep only the most recent events per trade cycle

        :param df: DataFrame
        :return: DataFrame
        """
    w = Window.partitionBy('trade_id', 'equity', 'price', 'size', 'participant_id').orderBy(desc('row_id'))
    return df \
        .withColumn('row_number', row_number().over(w)) \
        .filter(col("row_number") == 1) \
        .drop("row_number")


def slice_valid_trades(df):
    """
        # to filter only valid trades

        :param df: DataFrame
        :return: DataFrame
        """
    # this removes all records without a counterparty companion
    w = Window.partitionBy('trade_id', 'equity', 'price', 'size')
    no_orphans = df \
        .withColumn("count", count(col("participant_id")).over(w)) \
        .filter(col("count") == 2) \
        .drop("count")

    # this removes cancellations (both should be cancelled)
    return no_orphans \
        .withColumn("trade_types", collect_list(col("trade_report_type")).over(w)) \
        .filter(col("trade_types") != array(lit("Cancel"), lit("Cancel"))) \
        .drop("trade_types")


def prepare_report(df):
    """
        # performs grouping and prepares the report

        :param df: DataFrame
        :return: DataFrame
        """
    @udf(returnType=DecimalType(38, 2))
    def calculate(trades_list):
        total = Decimal("0.0")
        for trade in trades_list:
            if trade['side'] == "S":
                total += trade["amount"]
            else:
                total -= trade["amount"]
        return total

    return df \
        .withColumn("amount", col("price") * col("size")) \
        .withColumn("trades", struct(col("side"), col("amount"))) \
        .groupBy("participant_id") \
        .agg(collect_list(col("trades")).alias("trades")) \
        .withColumn("total", calculate(col("trades"))) \
        .drop("trades") \
        .sort("participant_id")


def transform(df):
    return df \
        .transform(populate_row_ids) \
        .transform(slice_last_trade_states_per_party) \
        .transform(slice_valid_trades) \
        .transform(prepare_report)