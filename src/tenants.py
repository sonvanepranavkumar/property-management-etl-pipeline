from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def transform_tenants(tenants_df):
    # Standardise tenant columns and data types
    transformed_df = (
        tenants_df
        .withColumn("Id", F.expr("try_cast(trim(Id) AS BIGINT)"))
        .withColumn("FirstName", F.trim(F.col("FirstName")))
        .withColumn("LastName", F.trim(F.col("LastName")))
        .withColumn("Email", F.lower(F.trim(F.col("Email"))))
        .withColumn(
            "Phone",
            F.when(F.trim(F.col("Phone")) == "", F.lit(None).cast("string")).otherwise(F.trim(F.col("Phone")))
        )
        .withColumn("LeaseStartDate", F.expr("try_cast(trim(LeaseStartDate) AS DATE)"))
        .withColumn("LeaseEndDate", F.expr("try_cast(trim(LeaseEndDate) AS DATE)"))
        .withColumn("PropertyId", F.expr("try_cast(trim(PropertyId) AS BIGINT)"))
    )

    print("Tenants transformation completed!")
    print(f"Total records: {transformed_df.count()}")

    return transformed_df

def validate_tenants(transformed_tenants_df):
    validated_df = (
        transformed_tenants_df
        
        # Flag missing or not positive tenant IDs
        .withColumn("invalid_tenant_id", F.col("Id").isNull() | (F.col("Id") <= 0))

        # Flag duplicate tenant IDs
        .withColumn("duplicate_tenant_id", F.count("*").over(Window.partitionBy("Id")) > 1)

        # Flag missing or over 100 character first names
        .withColumn(
            "invalid_first_name",
            F.col("FirstName").isNull()
            | (F.trim(F.col("FirstName")) == "")
            | (F.length(F.col("FirstName")) > 100)
        )

        # Flag missing or over 100 character last names
        .withColumn(
            "invalid_last_name",
            F.col("LastName").isNull()
            | (F.trim(F.col("LastName")) == "")
            | (F.length(F.col("LastName")) > 100)
        )

        # Flag missing or invalid email addresses
        .withColumn(
            "invalid_email",
            F.col("Email").isNull()
            | (F.trim(F.col("Email")) == "")
            | (~F.col("Email").rlike(
                r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$"
            ))
        )

        # Flag missing lease start dates
        .withColumn("invalid_lease_start_date", F.col("LeaseStartDate").isNull())

        # Flag missing lease end dates
        .withColumn("invalid_lease_end_date", F.col("LeaseEndDate").isNull())

        # Flag lease end dates before lease start dates
        .withColumn(
            "invalid_lease_period",
            F.col("LeaseStartDate").isNotNull() 
            & F.col("LeaseEndDate").isNotNull() 
            & (F.col("LeaseEndDate") < F.col("LeaseStartDate"))
        )

        # Flag missing or not positive property IDs
        .withColumn("invalid_property_id", F.col("PropertyId").isNull() | (F.col("PropertyId") <= 0))
    )

    print("Tenant validation completed!")

    return validated_df

def separate_tenants(validated_tenants_df):
    rejection_condition = (
        F.col("invalid_tenant_id")
        | F.col("duplicate_tenant_id")
        | F.col("invalid_first_name")
        | F.col("invalid_last_name")
        | F.col("invalid_email")
        | F.col("invalid_lease_start_date")
        | F.col("invalid_lease_end_date")
        | F.col("invalid_lease_period")
        | F.col("invalid_property_id")
    )

    # Filter rejected tenant records and add detailed reasons for each validation failure
    rejected_tenants_df = (
        validated_tenants_df
        .filter(rejection_condition)
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                ". ",
                F.when(F.col("invalid_tenant_id"), F.lit("Tenant Id is missing or not positive")),
                F.when(F.col("duplicate_tenant_id"), F.lit("Duplicate Tenant Id")),
                F.when(F.col("invalid_first_name"), F.lit("First Name is missing or exceeds 100 characters")),
                F.when(F.col("invalid_last_name"), F.lit("Last Name is missing or exceeds 100 characters")),
                F.when(F.col("invalid_email"), F.lit("Email is missing or is Invalid")),
                F.when(F.col("invalid_lease_start_date"), F.lit("Lease Start Date is missing")),
                F.when(F.col("invalid_lease_end_date"), F.lit("Lease End Date is missing")),
                F.when(F.col("invalid_lease_period"), F.lit("Lease End Date is before Lease Start Date")),
                F.when(F.col("invalid_property_id"), F.lit("Property Id is missing or not positive"))
            )
        )
    )

    # Filter clean tenant records
    clean_tenants_df = validated_tenants_df.filter(~rejection_condition)

    return clean_tenants_df, rejected_tenants_df