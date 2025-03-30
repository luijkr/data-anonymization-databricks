-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create a new catalog and schema

-- COMMAND ----------
-- DBTITLE 1,Create original schema
CREATE SCHEMA IF NOT EXISTS ${catalog_name_original}.${schema_name};

-- COMMAND ----------
-- DBTITLE 1,Create anonymized schema
CREATE SCHEMA IF NOT EXISTS ${catalog_name_anonyized}.${schema_name};
