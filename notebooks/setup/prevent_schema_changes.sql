-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Prevent schema changes in the anonymized data

-- COMMAND ----------
-- DBTITLE 1,Disable any automatic schema changes
ALTER TABLE ${catalog_name}.${schema_name}.dim_patients SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

ALTER TABLE ${catalog_name}.${schema_name}.dim_hospitals SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

ALTER TABLE ${catalog_name}.${schema_name}.dim_doctors SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

ALTER TABLE ${catalog_name}.${schema_name}.fact_visits SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);
