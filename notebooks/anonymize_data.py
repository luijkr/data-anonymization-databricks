# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generation
# MAGIC This notebook anonymizes the healthcare data model by hashing personally identifiable information (PII) columns as well as primary and foreign keys.

# COMMAND ----------
# DBTITLE 1,Import libraries
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
# DBTITLE 1,Retrieve job parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
schema_name_anonymized = dbutils.widgets.get("schema_name_anonymized")

# Define secret salt for hashing
secret_salt = "my_secret_salt"

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
# DBTITLE 1,Function to anonymize a table
def anonymize_table(df: DataFrame, column_names: list[str]) -> DataFrame:
    """Anonymize columns in a DataFrame."""
    return df.select(
        *[
            F.expr(f"sha2(concat(cast({c} as string), '{secret_salt}'), 256)").alias(c)
            if c in column_names
            else F.col(c)
            for c in df.columns
        ]
    )


# COMMAND ----------
# DBTITLE 1,Anonymize tables
for table_name in table_names:
    df = spark.table(f"{catalog_name}.{schema_name}.{table_name}")
    columns_to_anonymize = fetch_tag_columns(catalog_name, schema_name, "dim_patients")
    df_anonymized = anonymize_table(df, columns_to_anonymize)
    df_anonymized.write.mode("overwrite").saveAsTable(
        f"{catalog_name}.{schema_name_anonymized}.{table_name}"
    )
