from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DecimalType, DateType, StringType
from datetime import date
from decimal import Decimal
import rent_payments

spark = SparkSession.builder.getOrCreate()

def test_rent_payments_transform():

    # Arrange: create sample test data
    test_data = [
        (" 101 ", " 1200.50 ", "2024-01-01", " paid ", " 2001 ", " 101 "),
        ("102", "950.00", "2024-02-01", "pending", "2002", "102"),
        ("ABC", "Invalid", "Invalid", "Paid", "XYZ", "XYZ"),
        ("104", None, None, None, None, None),
        ("105", "1500.00", "2024-05-01", "", "2005", "2005")
    ]

    test_columns = [
        "Id", "Amount", "PaymentDate", "Status",
        "PropertyId", "TenantId"
    ]

    rent_payments_df = spark.createDataFrame(test_data, test_columns)

    # Act: run rent payments ETL transformation
    rent_payments_transformed_df = rent_payments.transform_rent_payments(rent_payments_df)

    # Assert: check output

    # Test data types
    rent_payments_schema = rent_payments_transformed_df.schema

    assert rent_payments_schema["Id"].dataType == LongType(), "Invalid Id data type"
    assert rent_payments_schema["Amount"].dataType == DecimalType(12, 2), "Invalid Amount data type"
    assert rent_payments_schema["PaymentDate"].dataType == DateType(), "Invalid PaymentDate data type"
    assert rent_payments_schema["Status"].dataType == StringType(), "Invalid Status data type"
    assert rent_payments_schema["PropertyId"].dataType == LongType(), "Invalid PropertyId data type"
    assert rent_payments_schema["TenantId"].dataType == LongType(), "Invalid TenantId data type"

    rent_payments_by_id = {
        row.Id: row
        for row in rent_payments_transformed_df.collect()
    }

    # Test trimming, capitalisation and numeric conversions
    assert rent_payments_by_id[101] == (
        101, Decimal("1200.50"), date(2024, 1, 1), "Paid", 2001, 101
    ), "Trimming, capitalisation or numeric conversion failed"

    assert rent_payments_by_id[102] == (
        102, Decimal("950.00"), date(2024, 2, 1), "Pending", 2002, 102
    ), "Trimming, capitalisation or numeric conversion failed"

    # Test invalid numeric values and dates become NULL
    assert rent_payments_transformed_df.filter(
        (F.col("Id").isNull()) & (F.col("Status") == "Paid")
    ).first() == (None, None, None, "Paid", None, None), "Invalid numeric value or date conversion failed"

    # Test existing NULL values are preserved
    assert rent_payments_by_id[104] == (104, None, None, None, None, None), "NULL preservation failed"

    # Test empty strings remain empty strings
    assert rent_payments_by_id[105] == (
        105, Decimal("1500.00"), date(2024, 5, 1), "", 2005, 2005
    ), "Empty strings preservation failed"

    # Test record count
    assert rent_payments_transformed_df.count() == 5, "Record count failed"

    print("All rent payments transformation tests finished!")