from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DecimalType, DateType, StringType
from datetime import date
from decimal import Decimal
import expenses

spark = SparkSession.builder.getOrCreate()

def test_expenses_transform():

    # Arrange: create sample test data
    test_data = [
        (" 101 ", " Repairs ", " Boiler repair ", " 250.50 ", "2024-01-01", " 2001 "),
        ("102", "Maintenance", " Garden maintenance ", "150.00", "2024-02-01", "2002"),
        ("ABC", "Cleaning", "Window cleaning", "Invalid", "Invalid", "XYZ"),
        ("104", None, None, None, None, None),
        ("105", "", "   ", "500.00", "2024-05-01", "2005")
    ]

    test_columns = ["Id", "Category", "Description", "Amount", "ExpenseDate", "PropertyId"]

    expenses_df = spark.createDataFrame(test_data, test_columns)

    # Act: run expenses etl transformation
    expenses_transformed_df = expenses.transform_expenses(expenses_df)

    # Assert: check output

    # Test data types
    expenses_schema = expenses_transformed_df.schema
    assert expenses_schema["Id"].dataType == LongType(), "Invalid Id data type"
    assert expenses_schema["Category"].dataType == StringType(), "Invalid Category data type"
    assert expenses_schema["Description"].dataType == StringType(), "Invalid Description data type"
    assert expenses_schema["Amount"].dataType == DecimalType(12, 2), "Invalid Amount data type"
    assert expenses_schema["ExpenseDate"].dataType == DateType(), "Invalid ExpenseDate data type"
    assert expenses_schema["PropertyId"].dataType == LongType(), "Invalid PropertyId data type"

    expenses_by_id = {
        row.Id: row
        for row in expenses_transformed_df.collect()
    }

    # Test trimming and numeric conversions
    assert expenses_by_id[101] == (
        101, "Repairs", "Boiler repair", Decimal("250.50"), date(2024, 1, 1), 2001
    ), "trimming or numeric conversion failed"

    assert expenses_by_id[102] == (
        102, "Maintenance", "Garden maintenance", Decimal("150.00"), date(2024, 2, 1), 2002
    ), "trimming or numeric conversion failed"

    # Test invalid numeric values become NULL
    assert expenses_transformed_df.filter(
        F.col("Category") == "Cleaning"
    ).first() == (None, "Cleaning", "Window cleaning", None, None, None), "Invalid numeric value conversion failed"

    # Test existing NULL values are preserved
    assert expenses_by_id[104] == (104, None, None, None, None, None), "NULL preservation failed"

    # Test empty descriptions become NULL and empty categories remain empty strings
    assert expenses_by_id[105] == (
        105, "", None, Decimal("500.00"), date(2024, 5, 1), 2005
    ), "Empty strings handling failed"

    # Test record count
    assert expenses_transformed_df.count() == 5, "record count failed"

    print("All expenses transformation tests finished!")

test_expenses_transform()