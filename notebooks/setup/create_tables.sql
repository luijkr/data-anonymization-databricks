-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create new tables

-- COMMAND ----------
-- DBTITLE 1,Create `patients` dimension table
CREATE OR REPLACE TABLE ${catalog_name}.${schema_name}.dim_patients (
    patient_id INT NOT NULL PRIMARY KEY,
    name STRING NOT NULL,
    dob DATE NOT NULL,
    ssn STRING NOT NULL,
    address STRING NOT NULL
);

-- COMMAND ----------
-- DBTITLE 1,Create `patients` hospitals table
CREATE OR REPLACE TABLE ${catalog_name}.${schema_name}.dim_hospitals (
    hospital_id INT NOT NULL PRIMARY KEY,
    name STRING NOT NULL,
    address STRING NOT NULL,
    phone STRING NOT NULL
);

-- COMMAND ----------
-- DBTITLE 1,Create `doctors` dimension table
CREATE OR REPLACE TABLE ${catalog_name}.${schema_name}.dim_doctors (
    doctor_id INT NOT NULL PRIMARY KEY,
    name STRING NOT NULL,
    specialization STRING NOT NULL,
    hospital_id INT NOT NULL,
    FOREIGN KEY (hospital_id) REFERENCES ${catalog_name}.${schema_name}.dim_hospitals(hospital_id)
);

-- COMMAND ----------
-- DBTITLE 1,Create `visits` dimension table
CREATE OR REPLACE TABLE ${catalog_name}.${schema_name}.fact_visits (
    visit_id INT NOT NULL PRIMARY KEY,
    patient_id INT NOT NULL,
    hospital_id INT NOT NULL,
    visit_date DATE NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES ${catalog_name}.${schema_name}.dim_patients(patient_id),
    FOREIGN KEY (hospital_id) REFERENCES ${catalog_name}.${schema_name}.dim_hospitals(hospital_id)
);
