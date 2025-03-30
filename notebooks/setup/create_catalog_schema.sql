-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create a new catalog and schema

-- COMMAND ----------
-- DBTITLE 1,Drop the catalogs if it exists
DROP CATALOG IF EXISTS ${catalog_name} CASCADE;

-- COMMAND ----------
-- DBTITLE 1,Create new catalogs
CREATE CATALOG ${catalog_name} MANAGED LOCATION 'abfss://renedev@saextazrevo001.dfs.core.windows.net';

-- COMMAND ----------
-- DBTITLE 1,Create new schema
CREATE SCHEMA IF NOT EXISTS ${catalog_name}.${schema_name};
