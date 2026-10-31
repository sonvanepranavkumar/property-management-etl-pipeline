from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, DateType
from datetime import date
import tenants

spark = SparkSession.builder.getOrCreate()

def test_tenants_transform():

    # Arrange: create sample test data
    test_data = [
        (" 101 ", " John ", " Smith ", " JOHN.SMITH@EXAMPLE.COM ", " 07123456789 ", "2024-01-01", "2025-01-01", " 2001 "),
        ("102", " Sarah ", " Jones ", " SARAH@EXAMPLE.COM ", " 07987654321 ", "2024-02-01", "2025-02-01", "2002"),
        ("ABC", "David", "Brown", " DAVID@EXAMPLE.COM ", "07111111111", "Invalid", "Invalid", "XYZ"),
        ("104", None, None, None, None, None, None, None),
        ("105", "", "", "", "", "2024-05-01", "2025-05-01", "2005")
    ]

    test_columns = [
        "Id", "FirstName", "LastName", "Email",
        "Phone", "LeaseStartDate", "LeaseEndDate", "PropertyId"
    ]

    tenants_df = spark.createDataFrame(test_data, test_columns)

    # Act: run tenants ETL transformation
    tenants_transformed_df = tenants.transform_tenants(tenants_df)

    # Assert: check output

    # Test data types
    tenants_schema = tenants_transformed_df.schema
    assert tenants_schema["Id"].dataType == LongType(), "Invalid Id data type"
    assert tenants_schema["FirstName"].dataType == StringType(), "Invalid FirstName data type"
    assert tenants_schema["LastName"].dataType == StringType(), "Invalid LastName data type"
    assert tenants_schema["Email"].dataType == StringType(), "Invalid Email data type"
    assert tenants_schema["Phone"].dataType == StringType(), "Invalid Phone data type"
    assert tenants_schema["LeaseStartDate"].dataType == DateType(), "Invalid LeaseStartDate data type"
    assert tenants_schema["LeaseEndDate"].dataType == DateType(), "Invalid LeaseEndDate data type"
    assert tenants_schema["PropertyId"].dataType == LongType(), "Invalid PropertyId data type"

    tenants_by_id = {
        row.Id: row
        for row in tenants_transformed_df.collect()
    }

    # Test trimming, lowercase emails and numeric conversions
    assert tenants_by_id[101] == (
        101, "John", "Smith", "john.smith@example.com", "07123456789", date(2024, 1, 1), date(2025, 1, 1), 2001
    ), "trimming, lowercase conversion or numeric conversion failed"

    assert tenants_by_id[102] == (
        102, "Sarah", "Jones", "sarah@example.com", "07987654321", date(2024, 2, 1), date(2025, 2, 1), 2002
    ), "trimming, lowercase conversion or numeric conversion failed"

    # Test invalid numeric values and dates become NULL
    assert tenants_transformed_df.filter(
        F.col("FirstName") == "David"
    ).first() == (None, "David", "Brown", "david@example.com", "07111111111", None, None, None), "Invalid numeric value or date conversion failed"

    # Test existing NULL values are preserved
    assert tenants_by_id[104] == (104, None, None, None, None, None, None, None), "NULL preservation failed"

    # Test empty strings remain empty strings, except Phone
    assert tenants_by_id[105] == (105, "", "", "", None,date(2024, 5, 1), date(2025, 5, 1), 2005), "Empty strings or phone cleaning failed"

    # Test record count
    assert tenants_transformed_df.count() == 5, "Record count failed"

    print("All tenants transformation tests finished!")

test_tenants_transform()