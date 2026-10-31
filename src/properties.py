from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def transform_properties(properties_df):
    # Standardise property columns and data types
    transformed_df = (
        properties_df
        .withColumn("Id", F.expr("try_cast(trim(Id) AS BIGINT)"))
        .withColumn("Address", F.trim(F.col("Address")))
        .withColumn("Rent", F.expr("try_cast(trim(Rent) AS DECIMAL(12,2))"))
        .withColumn("Status", F.initcap(F.trim(F.col("Status"))))
        .withColumn("LandlordId", F.expr("try_cast(trim(LandlordId) AS BIGINT)"))
    )

    print("Properties transformation completed!")
    print(f"Total records: {transformed_df.count()}")

    return transformed_df

def validate_properties(transformed_properties_df):
    validated_properties_df = (
        transformed_properties_df
        
        # Flag missing or not positive property IDs
        .withColumn("invalid_property_id", F.col("Id").isNull() | (F.col("Id") <= 0))
        
        # Flag duplicate property IDs
        .withColumn("duplicate_property_id", F.count("*").over(Window.partitionBy("Id")) > 1)

        # Flag missing or not positive rent
        .withColumn("invalid_rent", F.col("Rent").isNull() | (F.col("Rent") <= 0))

        # Flag missing or over 200 character addresses
        .withColumn(
            "invalid_address",
            F.col("Address").isNull()
            | (F.col("Address") == "")
            | (F.length("Address") > 200)
        )

        # Flag missing or invalid property status
        .withColumn("invalid_status", F.col("Status").isNull() | (~F.col("Status").isin("Occupied", "Vacant")))

        # Flag missing or not positive landlord IDs
        .withColumn("invalid_landlord_id", F.col("LandlordId").isNull() | (F.col("LandlordId") <= 0))
    )

    print("Property validation completed!")

    return validated_properties_df

def separate_properties(validated_properties_df):
    rejection_condition = (
        F.col("invalid_property_id")
        | F.col("duplicate_property_id")
        | F.col("invalid_rent")
        | F.col("invalid_address")
        | F.col("invalid_status")
        | F.col("invalid_landlord_id")
    )

    # Filter rejected property records and add detailed reasons for each validation failure
    rejected_properties_df = (
        validated_properties_df
        .filter(rejection_condition)
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                ". ",
                F.when(F.col("invalid_property_id"), F.lit("Property Id is missing or not positive")),
                F.when(F.col("duplicate_property_id"), F.lit("Duplicate Property Id")),
                F.when(F.col("invalid_rent"), F.lit("Rent is missing or not positive")),
                F.when(F.col("invalid_address"), F.lit("Address is missing or exceeds 200 characters")),
                F.when(F.col("invalid_status"), F.lit("Status is missing or is not Occupied or Vacant")),
                F.when(F.col("invalid_landlord_id"), F.lit("Landlord Id is missing or not positive"))
            )
        )
    )

    # Filter clean property records
    clean_properties_df = validated_properties_df.filter(~rejection_condition)

    return clean_properties_df, rejected_properties_df