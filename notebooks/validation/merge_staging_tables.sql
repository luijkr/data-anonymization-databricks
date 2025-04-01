-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Merge the staging tables into the final tables

-- COMMAND ----------
-- DBTITLE 1,Merge `dim_patients` table
MERGE INTO ${catalog_name}_anonymized.${schema_name}.dim_patients AS target
USING ${catalog_name}_anonymized_staging.${schema_name}.dim_patients AS source
ON target.patient_id = source.patient_id
WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.dob = source.dob,
        target.ssn = source.ssn,
        target.address = source.address
WHEN NOT MATCHED THEN
    INSERT (patient_id, name, dob, ssn, address)
    VALUES (source.patient_id, source.name, source.dob, source.ssn, source.address);

-- COMMAND ----------
-- DBTITLE 1,Merge `dim_hospitals` table
MERGE INTO ${catalog_name}_anonymized.${schema_name}.dim_hospitals AS target
USING ${catalog_name}_anonymized_staging.${schema_name}.dim_hospitals AS source
ON target.hospital_id = source.hospital_id
WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.address = source.address,
        target.phone = source.phone
WHEN NOT MATCHED THEN
    INSERT (hospital_id, name, address, phone)
    VALUES (source.hospital_id, source.name, source.address, source.phone);

-- COMMAND ----------
-- DBTITLE 1,Merge `dim_doctors` table
MERGE INTO ${catalog_name}_anonymized.${schema_name}.dim_doctors AS target
USING ${catalog_name}_anonymized_staging.${schema_name}.dim_doctors AS source
ON target.doctor_id = source.doctor_id
WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.specialization = source.specialization,
        target.hospital_id = source.hospital_id
WHEN NOT MATCHED THEN
    INSERT (doctor_id, name, specialization, hospital_id)
    VALUES (source.doctor_id, source.name, source.specialization, source.hospital_id);

-- COMMAND ----------
-- DBTITLE 1,Merge `fact_visits` table
MERGE INTO ${catalog_name}_anonymized.${schema_name}.fact_visits AS target
USING ${catalog_name}_anonymized_staging.${schema_name}.fact_visits AS source
ON target.visit_id = source.visit_id
WHEN MATCHED THEN
    UPDATE SET
        target.patient_id = source.patient_id,
        target.hospital_id = source.hospital_id,
        target.visit_date = source.visit_date
WHEN NOT MATCHED THEN
    INSERT (visit_id, patient_id, hospital_id, visit_date)
    VALUES (source.visit_id, source.patient_id, source.hospital_id, source.visit_date);
