# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generation
# MAGIC This notebook anonymizes the healthcare data model by hashing columns containing
# MAGIC personally identifiable information (PII) as well as primary and foreign keys.

# COMMAND ----------
# DBTITLE 1,Import libraries
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from math import ceil


# COMMAND ----------
# DBTITLE 1,Retrieve job parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Define secret salt for hashing
secret_salt = dbutils.secrets.get(
    scope="data-anonymization",
    key="secret-salt"
)

# COMMAND ----------
# DBTITLE 1,Load system table for the column tags
column_tags_df = spark.table("system.information_schema.column_tags")


# COMMAND ----------
# DBTITLE 1,Fetch columns to anonymize
def fetch_tag_columns(catalog_name: str, schema_name: str, table_name: str) -> list[str]:
    """Fetch columns to anonymize based on tags."""
    customer_columns = column_tags_df.select("column_name").where(
        (F.col("catalog_name") == catalog_name)
        & (F.col("schema_name") == schema_name)
        & (F.col("table_name") == table_name)
        & (F.col("tag_name").isNotNull())
    )
    return [row.column_name for row in customer_columns.collect()]


# COMMAND ----------
# DBTITLE 1,Define tables to anonymize
table_names = [table.name for table in spark.catalog.listTables(f"{catalog_name}.{schema_name}")]

# COMMAND ----------
# DBTITLE 1,Define transformations
def add_salt(column_name: str, salt: str = secret_salt) -> F.col:
    """Add a secret salt to a column."""
    return F.concat(F.col(column_name).cast("string"), F.lit(salt))


def salted_hash(column_name: str) -> F.col:
    """Hash a column with a secret salt."""
    salted_column = add_salt(column_name)
    return F.sha2(salted_column, 256)


def anonymize_integer(column_name: str, max_value: int = 1000000) -> F.col:
    """Anonymize an integer column by hashing and taking modulo."""
    max_modulo_number = ceil(max_value / 2)
    salted_column = add_salt(column_name)
    anonymized_integer = (F.hash(salted_column) % max_modulo_number).cast("int")

    # Ensure the anonymized integer is non-negative
    anonymized_integer = anonymized_integer + F.lit(max_modulo_number)
    return anonymized_integer


def anonymize_date(column_name: str, max_days_shift: int = 365) -> F.col:
    """Anonymize a date column by adding a random number of days."""
    max_modulo_days = ceil(max_days_shift)
    salted_column = add_salt(column_name)

    # Generate a random number of days to shift the date from -max_days_shift to +max_days_shift
    random_days = (F.hash(salted_column) % max_modulo_days).cast("int")

    # Ensure the random days are non-negative to avoid future dates
    day_shift = random_days - F.lit(max_days_shift)
    return F.date_add(column_name, day_shift)


def transform_column(column_name: str, data_type: T.StructType) -> F.col:
    """Transform a column based on its data type."""
    match data_type:
        case T.StringType():
            return salted_hash(column_name)
        case T.DateType():
            return anonymize_date(column_name)
        case T.IntegerType():
            return anonymize_integer(column_name)
        case _:
            return F.col(column_name)

# COMMAND ----------
# DBTITLE 1,Function to anonymize a table
def anonymize_table(df: DataFrame, tagged_columns: list[str]) -> DataFrame:
    """Anonymize tagged columns in a DataFrame."""
    return df.select(
        *[
            transform_column(c.name, c.dataType).alias(c.name)
            if c.name in tagged_columns
            else F.col(c.name)
            for c in df.schema
        ]
    )

# COMMAND ----------
# DBTITLE 1,Anonymize tables
for table_name in table_names:
    df = spark.table(f"{catalog_name}.{schema_name}.{table_name}")

    # Only anonymize columns with the appropriate tags
    columns_to_anonymize = fetch_tag_columns(catalog_name, schema_name, table_name)

    df_anonymized = anonymize_table(df, columns_to_anonymize)
    df_anonymized.write.mode("overwrite").saveAsTable(
        f"{catalog_name}_anonymized_staging.{schema_name}.{table_name}"
    )
