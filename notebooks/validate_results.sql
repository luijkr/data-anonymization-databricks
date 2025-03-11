-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Validate the results of the data anonymization

-- COMMAND ----------
-- DBTITLE 1,Check raw counts for dimensional tables
WITH
    stats AS (
        SELECT 
            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name}.dim_doctors) AS num_docs,
            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name_anonymized}.dim_doctors) AS num_docs_anon,

            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name}.dim_patients) AS num_patients,
            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name_anonymized}.dim_patients) AS num_patients_anon,

            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name}.dim_treatments) AS num_treatments,
            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name_anonymized}.dim_treatments) AS num_treatments_anon,

            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name}.dim_hospitals) AS num_hospitals,
            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name_anonymized}.dim_hospitals) AS num_hospitals_anon
  )
SELECT
    assert_true(num_docs = num_docs_anon, 'Unequal number of rows in `dim_doctors` tables'),
    assert_true(num_patients = num_patients_anon, 'Unequal number of rows in `dim_patients` tables'),
    assert_true(num_treatments = num_treatments_anon, 'Unequal number of rows in `dim_treatments` tables'),
    assert_true(num_hospitals = num_hospitals_anon, 'Unequal number of rows in `dim_hospitals` tables')
FROM stats;

-- COMMAND ----------
-- DBTITLE 1,Check raw counts for fact tables
WITH
    stats AS (
        SELECT 
            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name}.fact_visits) AS num_visits,
            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name_anonymized}.fact_visits) AS num_visits_anon,

            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name}.fact_treatments) AS num_treatments,
            (SELECT COUNT(*) FROM ${catalog_name}.${schema_name_anonymized}.fact_treatments) AS num_treatments_anon
  )
SELECT
    assert_true(num_visits = num_visits_anon, 'Unequal number of rows in `fact_visits` tables'),
    assert_true(num_treatments = num_treatments_anon, 'Unequal number of rows in `fact_treatments` tables')
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
        FROM ${catalog_name}.${schema_name_anonymized}.fact_visits fv
        JOIN ${catalog_name}.${schema_name_anonymized}.dim_patients dp
            ON fv.patient_id = dp.patient_id
        JOIN ${catalog_name}.${schema_name_anonymized}.dim_hospitals dh
            ON fv.hospital_id = dh.hospital_id
    ),

    stats AS (
        SELECT 
            (SELECT num_rows FROM raw) AS num_rows,
            (SELECT num_rows FROM anonymized) AS num_rows_anon
    )

SELECT
    assert_true(num_rows = 5000, 'Joining `fact_visits` with `dim_patients` and `dim_hospitals` did not return 5000 rows'),
    assert_true(num_rows = num_rows_anon, 'Unequal number of rows in `fact_visits` tables joined on all dimensions')
FROM stats;

-- COMMAND ----------
-- DBTITLE 1,Check referential integrity for fact table `visits`
WITH
    raw AS (
        SELECT COUNT(*) AS num_rows
        FROM ${catalog_name}.${schema_name}.fact_treatments ft
        JOIN ${catalog_name}.${schema_name}.dim_patients dp
            ON ft.patient_id = dp.patient_id
        JOIN ${catalog_name}.${schema_name}.dim_doctors dd
            ON ft.doctor_id = dd.doctor_id
        JOIN ${catalog_name}.${schema_name}.dim_treatments dt
            ON ft.treatment_id = dt.treatment_id
    ),

    anonymized AS (
        SELECT COUNT(*) AS num_rows
        FROM ${catalog_name}.${schema_name_anonymized}.fact_treatments ft
        JOIN ${catalog_name}.${schema_name_anonymized}.dim_patients dp
            ON ft.patient_id = dp.patient_id
        JOIN ${catalog_name}.${schema_name_anonymized}.dim_doctors dd
            ON ft.doctor_id = dd.doctor_id
        JOIN ${catalog_name}.${schema_name_anonymized}.dim_treatments dt
            ON ft.treatment_id = dt.treatment_id
    ),

    stats AS (
        SELECT 
            (SELECT num_rows FROM raw) AS num_rows,
            (SELECT num_rows FROM anonymized) AS num_rows_anon
    )

SELECT
    assert_true(num_rows = 10000, 'Joining `fact_treatments` with `dim_patients`, `dim_doctors`, and `dim_treatments` did not return 10000 rows'),
    assert_true(num_rows = num_rows_anon, 'Unequal number of rows in `fact_treatments` tables joined on all dimensions')
FROM stats;

-- COMMAND ----------
-- DBTITLE 1,Check referential integrity for fact tables joined together
WITH
    raw AS (
        SELECT COUNT(*) AS num_rows
        FROM ${catalog_name}.${schema_name}.fact_treatments ft
        JOIN ${catalog_name}.${schema_name}.fact_visits fv
            ON ft.visit_id = fv.visit_id
    ),

    anonymized AS (
        SELECT COUNT(*) AS num_rows
        FROM ${catalog_name}.${schema_name_anonymized}.fact_treatments ft
        JOIN ${catalog_name}.${schema_name_anonymized}.fact_visits fv
            ON ft.visit_id = fv.visit_id
    ),

    stats AS (
        SELECT 
            (SELECT num_rows FROM raw) AS num_rows,
            (SELECT num_rows FROM anonymized) AS num_rows_anon
    )

SELECT
    assert_true(num_rows = 10000, 'Joining `fact_treatments` with `fact_visits` did not return 10000 rows'),
    assert_true(num_rows = num_rows_anon, 'Unequal number of rows in `fact_treatments` tables joined on `fact_visits`')
FROM stats;
