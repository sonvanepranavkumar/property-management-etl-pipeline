from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DecimalType, StringType
from decimal import Decimal
import properties

spark = SparkSession.builder.getOrCreate()

def test_properties_transform():

    # Arrange: create sample test data
    test_data = [
        (" 101 ", " 25 High Street ", "1200.50", " Vacant ", " 2001 "),
        ("102", "10 London Road", "950", "occupied", "2002"),
        ("ABC", "5 Park Lane", "Unknown", "Occupied", "XYZ"),
        ("104", None, None, None, None),
        ("105", "", "1500", "", "2005")
    ]

    test_columns = ["Id", "Address", "Rent", "Status", "LandlordId"]

    properties_df = spark.createDataFrame(test_data, test_columns)

    # Act: run properties etl transformation
    properties_transformed_df = properties.transform_properties(properties_df)

    # Assert: check output

    #Test data types
    properties_schema = properties_transformed_df.schema
    assert properties_schema["Id"].dataType == LongType(), "Invalid Id data type"
    assert properties_schema["Address"].dataType == StringType(), "Invalid Address data type"
    assert properties_schema["Rent"].dataType == DecimalType(12, 2), "Invalid Rent data type"
    assert properties_schema["Status"].dataType == StringType(), "Invalid Status data type"
    assert properties_schema["LandlordId"].dataType == LongType(), "Invalid LandlordId data type"

    properties_by_id = {
        row.Id: row
        for row in properties_transformed_df.collect()
    }

    #Test trimming, capitalisation and numeric conversions
    assert properties_by_id[101] == (101, "25 High Street", Decimal("1200.50"), "Vacant", 2001), "trimming, capitalisation or numeric conversion failed"
    assert properties_by_id[102] == (102, "10 London Road", Decimal("950.00"), "Occupied", 2002), "trimming, capitalisation or numeric conversion failed"

    #Test invalid numeric values become NULL
    assert properties_transformed_df.filter(
        F.col("Address") == "5 Park Lane"
    ).first() == (None, "5 Park Lane", None, "Occupied", None), "Invalid numeric value conversion failed"

    #Test existing NULL values are preserved
    assert properties_by_id[104] == (104, None, None, None, None), "NULL preservation failed"

    #Test empty strings remain empty strings
    assert properties_by_id[105] == (105, "", Decimal("1500.00"), "", 2005), "empty strings preservation failed"

    #Test record count
    assert properties_transformed_df.count() == 5, "record count failed"

    print("All properties transformation tests finished!")

test_properties_transform()