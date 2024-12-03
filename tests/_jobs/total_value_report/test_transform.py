from decimal import Decimal
from pyspark.testing.utils import assertDataFrameEqual
from jobs.total_value_report.transform import transform, slice_last_trade_states_per_party
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from jobs.total_value_report.extract import schema


# Example of a unit test
def test_slice_last_trade_states_per_party(spark_fixture):
    sample_data = [
        {"row_id": 1, "trade_id": "tradeA", "equity": "ABC", "price": 10, "size": 1, "participant_id": "participant_a",
         "trade_report_type": "New"},
        {"row_id": 2, "trade_id": "tradeA", "equity": "ABC", "price": 10, "size": 1, "participant_id": "participant_a",
         "trade_report_type": "Canceled"},
        {"row_id": 3, "trade_id": "tradeB", "equity": "ABC", "price": 20, "size": 2, "participant_id": "participant_b",
         "trade_report_type": "New"},
        {"row_id": 4, "trade_id": "tradeB", "equity": "ABC", "price": 20, "size": 2, "participant_id": "participant_b",
         "trade_report_type": "Canceled"},
    ]
    original_df = spark_fixture.createDataFrame(sample_data)
    transformed_df = slice_last_trade_states_per_party(original_df)

    expected_data = [
        {"row_id": 2, "trade_id": "tradeA", "equity": "ABC", "price": 10, "size": 1, "participant_id": "participant_a",
         "trade_report_type": "Canceled"},
        {"row_id": 4, "trade_id": "tradeB", "equity": "ABC", "price": 20, "size": 2, "participant_id": "participant_b",
         "trade_report_type": "Canceled"},
    ]
    expected_df = spark_fixture.createDataFrame(expected_data)
    assertDataFrameEqual(transformed_df, expected_df)


# and so on...

# A complete pipeline test
def test_transform(spark_fixture):
    original_df = spark_fixture.read.csv(
        "./resources/trades.csv",
        header=True,
        mode="DROPMALFORMED",
        schema=schema
    )
    transformed_df = transform(original_df)

    expected_schema = StructType([
        StructField('participant_id', StringType(), True),
        StructField('total', DecimalType(38,2), True)
    ])
    expected_data = [
        {"participant_id": "Firm 1", "total": Decimal(-2213.25)},
        {"participant_id": "Firm 2", "total": Decimal(-575.00)},
        {"participant_id": "Firm 3", "total": Decimal(-2947.50)},
        {"participant_id": "Firm 4", "total": Decimal(-1430.00)},
        {"participant_id": "Firm 5", "total": Decimal(-501.75)},
        {"participant_id": "Firm 6", "total": Decimal(-429.00)},
        {"participant_id": "Firm 7", "total": Decimal(8096.50)},
    ]
    expected_df = spark_fixture.createDataFrame(expected_data, schema=expected_schema)
    assertDataFrameEqual(transformed_df, expected_df)