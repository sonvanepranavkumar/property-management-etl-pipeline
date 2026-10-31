from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def extract_property_management_data(spark, source_path):
    source_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(source_path)
    )

    print(f"{source_path} extracted successfully!")

    # Log the number of source records
    print(f"{source_path} extracted: {source_df.count()} rows")

    return source_df

def upsert_property_management_table(spark, source_df, target_table, key_column="Id"):
    if not spark.catalog.tableExists(target_table):
        # Create property management table
        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        print(f"Property management table created: {target_table}")

    else:
        # Upsert property management table
        target = DeltaTable.forName(spark, target_table)

        (
            target.alias("target")
            .merge(
                source_df.alias("source"),
                f"target.{key_column} = source.{key_column}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Property management table upsert completed: {target_table}")

def summarise_property_management_etl(source_df, clean_df, rejected_df, source_table):
    total_count = source_df.count()
    clean_count = clean_df.count()
    rejected_count = rejected_df.count()

    # Calculate the clean record success rate
    success_rate = (
        clean_count / total_count * 100
        if total_count > 0
        else 0
    )

    print("=" * 45)
    print("ETL SUMMARY")
    print("=" * 45)
    print(f"Source: {source_table}")
    print(f"Total records: {total_count}")
    print(f"Clean records: {clean_count}")
    print(f"Rejected records: {rejected_count}")
    print(f"Success rate: {success_rate:.2f}%")
    print("=" * 45)

    return {
        "total": total_count,
        "clean": clean_count,
        "rejected": rejected_count,
        "success_rate": success_rate
    }