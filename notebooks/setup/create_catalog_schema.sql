-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create a new catalog and schema

-- COMMAND ----------
-- DBTITLE 1,Create new catalogs
CREATE CATALOG IF NOT EXISTS ${catalog_name}
MANAGED LOCATION 'abfss://renedev@saextazrevo001.dfs.core.windows.net';

-- COMMAND ----------
-- DBTITLE 1,Drop the existing schema
DROP SCHEMA IF EXISTS ${catalog_name}.${schema_name} CASCADE;

-- COMMAND ----------
-- DBTITLE 1,Create new schema
CREATE SCHEMA ${catalog_name}.${schema_name};
