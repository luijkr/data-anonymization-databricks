-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Validate the results of the data anonymization

-- COMMAND ----------
-- DBTITLE 1,Check raw counts for dimensional tables
WITH
    stats AS (
        SELECT
            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name}.dim_doctors) AS num_docs,
            (SELECT COUNT(*) FROM ${catalog_name}_anonymized_staging.${schema_name}.dim_doctors) AS num_docs_anon,

            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name}.dim_patients) AS num_patients,
            (SELECT COUNT(*) FROM ${catalog_name}_anonymized_staging.${schema_name}.dim_patients) AS num_patients_anon,

            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name}.dim_hospitals) AS num_hospitals,
            (SELECT COUNT(*) FROM ${catalog_name}_anonymized_staging.${schema_name}.dim_hospitals) AS num_hospitals_anon
  )
SELECT
    assert_true(num_docs = num_docs_anon, 'Unequal number of rows in `dim_doctors` tables'),
    assert_true(num_patients = num_patients_anon, 'Unequal number of rows in `dim_patients` tables'),
    assert_true(num_hospitals = num_hospitals_anon, 'Unequal number of rows in `dim_hospitals` tables')
FROM stats;

-- COMMAND ----------
-- DBTITLE 1,Check raw counts for fact tables
WITH
    stats AS (
        SELECT
            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name}.fact_visits) AS num_visits,
            (SELECT COUNT(*) FROM ${catalog_name}_anonymized_staging.${schema_name}.fact_visits) AS num_visits_anon
  )
SELECT
    assert_true(num_visits = num_visits_anon, 'Unequal number of rows in `fact_visits` tables')
FROM stats;

-- COMMAND ----------
-- DBTITLE 1,Check referential integrity for fact table `visits`
WITH
    raw AS (
        SELECT COUNT(*) AS num_rows
        FROM ${catalog_name}.${schema_name}.fact_visits fv
        JOIN ${catalog_name}.${schema_name}.dim_patients dp
            ON fv.patient_id = dp.patient_id
        JOIN ${catalog_name}.${schema_name}.dim_hospitals dh
            ON fv.hospital_id = dh.hospital_id
    ),

    anonymized AS (
        SELECT COUNT(*) AS num_rows
        FROM ${catalog_name}_anonymized_staging.${schema_name}.fact_visits fv
        JOIN ${catalog_name}_anonymized_staging.${schema_name}.dim_patients dp
            ON fv.patient_id = dp.patient_id
        JOIN ${catalog_name}_anonymized_staging.${schema_name}.dim_hospitals dh
            ON fv.hospital_id = dh.hospital_id
    ),

    stats AS (
        SELECT
            (SELECT num_rows FROM raw) AS num_rows,
            (SELECT num_rows FROM anonymized) AS num_rows_anon
    )

SELECT
    assert_true(num_rows = num_rows_anon, 'Unequal number of rows in `fact_visits` tables joined on all dimensions')
FROM stats;
