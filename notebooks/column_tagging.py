# Databricks notebook source
# MAGIC %md
# MAGIC # Column tagging
# MAGIC This notebook adds tags to columns in the healthcare data model. The tags are used to identify primary keys and personally identifiable information (PII) columns.

# COMMAND ----------
# DBTITLE 1,Import libraries
import pyspark.sql.functions as F

# COMMAND ----------
# DBTITLE 1,Retrieve job parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------
# DBTITLE 1,Configuration
pii_columns = {"name", "dob", "ssn", "address", "phone"}
pii_regex = "|".join(pii_columns)


# COMMAND ----------
# DBTITLE 1,Create SQL query to add tags
def create_tag_sql_query(table_identifier: str, column_name: str, tag: str) -> str:
    """Returns a SQL query to add a tag to a column."""
    return f"ALTER TABLE {table_identifier} ALTER COLUMN {column_name} SET TAGS ('{tag}')"


# COMMAND ----------
# DBTITLE 1,Define function to add tags to columns
def add_tags(table_identifier: str, column_names: list[str], tag: str) -> None:
    """Add tags to columns in a DataFrame."""
    for column_name in column_names:
        query = create_tag_sql_query(table_identifier, column_name, tag)
        spark.sql(query)


# COMMAND ----------
# DBTITLE 1,Prerequisites for adding tags
# System tables used in tagging
key_column_usage = spark.table("system.information_schema.key_column_usage").where(
    (F.col("table_catalog") == catalog_name) & (F.col("table_schema") == schema_name)
)
columns = spark.table("system.information_schema.columns").where(
    (F.col("table_catalog") == catalog_name) & (F.col("table_schema") == schema_name)
)

# All tables in schema
table_names = [table.name for table in spark.catalog.listTables(f"{catalog_name}.{schema_name}")]

key_tag = "is_key"
pii_tag = "is_pii"
pii_columns = {"name", "dob", "ssn", "address", "phone"}
pii_regex = "|".join(pii_columns)

# COMMAND ----------
# DBTITLE 1,Loop over tables to add tags to columns in tables
for table_name in table_names:
    table_identifier = f"{catalog_name}.{schema_name}.{table_name}"

    # Add key tags
    key_column_names = key_column_usage.where(F.col("table_name") == table_name).collect()
    key_column_names = [row.column_name for row in key_column_names]
    add_tags(table_identifier, key_column_names, key_tag)

    # Add PII tags
    pii_column_names = columns.where(
        (F.col("table_name") == table_name) & (F.col("column_name").rlike(pii_regex))
    ).collect()
    pii_column_names = [row.column_name for row in pii_column_names]
    add_tags(table_identifier, pii_column_names, pii_tag)
