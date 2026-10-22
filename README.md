# Property Management ETL Pipeline

An end to end data engineering project built using Python, PySpark, Databricks, Delta Lake and Power BI to process and analyse property management data.

## Features

- ETL pipeline for property, tenant, rent payment and expense datasets
- Data ingestion from CSV landing zone files
- Data cleansing, standardisation and schema validation
- Data quality checks for missing values, duplicate IDs, invalid formats and out of range values
- Lease date validation and rejection reason tracking
- Separation of clean and rejected records
- Medallion Architecture using Bronze, Silver and Gold layers
- Delta Lake tables for structured data storage
- Gold layer analytics for property occupancy, average lease duration, rent payments and expenses
- Databricks Jobs for pipeline orchestration
- Power BI dashboard for data quality reporting and analysis

## Technology Stack

- Python
- PySpark
- SQL
- Databricks
- Delta Lake
- Power BI

## Architecture

- **Bronze Layer:** Ingests raw data from CSV landing files
- **Silver Layer:** Cleans, standardises and validates data separating clean and rejected records
- **Gold Layer:** Stores analytics ready datasets for property management reporting
- **Power BI:** Visualises data quality metrics

## Getting Started

### Prerequisites

- Access to Databricks
- Python and PySpark
- Delta Lake support
- Power BI Desktop
- Property management CSV datasets

### Installation

1. Clone the repository
2. Import the Databricks notebooks into your workspace
3. Upload the CSV files
4. Configure the input paths and Delta Lake table locations
5. Run the ETL notebooks in the required order
6. Configure Databricks Jobs to orchestrate pipeline execution
7. Connect Power BI to the processed data

## Data Quality

The pipeline applies validation rules to identify:

- Missing or null values
- Duplicate identifiers
- Invalid data formats and data types
- Out of range values
- Inconsistent lease dates

Invalid records are separated from clean records with rejection reasons captured to support data quality monitoring and troubleshooting.

## Analytics

The Gold layer supports analysis of:

- Property occupancy
- Average lease duration
- Rent payment activity
- Property expenses

## Reporting

A Power BI dashboard provides data quality reporting through cards, charts and summary tables helping users analyse clean and rejected records.

## Future Improvements

- Automated unit testing for data transformations
- Enhanced pipeline logging and error handling
- Incremental data processing
- Automated data quality monitoring and alerts
- CI/CD for pipeline deployment
