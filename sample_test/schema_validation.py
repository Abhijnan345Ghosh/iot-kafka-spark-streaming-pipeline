from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql import DataFrame

#Expected Schema Definition
expected_schema = StructType() \
    .add("trip_id", StringType()) \
    .add("car_id", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("timestamp", TimestampType()) \
    .add("speed_kmph", DoubleType()) \
    .add("fuel_level", DoubleType()) \
    .add("engine_temp_c", DoubleType()) \
    .add("trip_start_time", TimestampType()) \
    .add("trip_start_latitude", DoubleType()) \
    .add("trip_start_longitude", DoubleType())

# Schema Comparison Function
def compare_with_expected_schema(df: DataFrame):
    actual_schema = df.schema  # df is a DataFrame, so .schema is valid here
    expected_fields = {f.name: f.dataType for f in expected_schema}
    actual_fields = {f.name: f.dataType for f in actual_schema}
    all_keys = set(expected_fields) | set(actual_fields)

    discrepancies = []
    for key in all_keys:
        if key not in actual_fields:
            discrepancies.append(f"Missing column in incoming data: {key}")
        elif key not in expected_fields:
            discrepancies.append(f"Unexpected column in incoming data: {key}")
        elif expected_fields[key] != actual_fields[key]:
            discrepancies.append(f"Type mismatch in column '{key}': expected {expected_fields[key]}, got {actual_fields[key]}")
    return discrepancies
