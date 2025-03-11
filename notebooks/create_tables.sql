-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create a new catalog and new schemas

-- COMMAND ----------

DROP CATALOG ${catalog_name} CASCADE;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ${catalog_name};

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${catalog_name}.${schema_name};

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${catalog_name}.${schema_name_anonymized};

-- COMMAND ----------

USE ${catalog_name}.${schema_name};

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_patients (
    patient_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    dob DATE NOT NULL,
    ssn VARCHAR(20) NOT NULL,
    address STRING NOT NULL
);

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_hospitals (
    hospital_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address STRING NOT NULL,
    phone VARCHAR(50) NOT NULL
);

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_doctors (
    doctor_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    specialization VARCHAR(255) NOT NULL,
    hospital_id INT NOT NULL,
    FOREIGN KEY (hospital_id) REFERENCES dim_hospitals(hospital_id)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_visits (
    visit_id INT PRIMARY KEY,
    patient_id INT NOT NULL,
    hospital_id INT NOT NULL,
    visit_date DATE NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES dim_patients(patient_id),
    FOREIGN KEY (hospital_id) REFERENCES dim_hospitals(hospital_id)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_treatments (
    treatment_id INT PRIMARY KEY,
    treatment_name VARCHAR(255) NOT NULL,
    cost DOUBLE NOT NULL
);

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_treatments (
    treatment_record_id INT PRIMARY KEY,
    patient_id INT NOT NULL,
    visit_id INT NOT NULL,
    doctor_id INT NOT NULL,
    treatment_id INT NOT NULL,
    treatment_date DATE NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES dim_patients(patient_id),
    FOREIGN KEY (visit_id) REFERENCES fact_visits(visit_id),
    FOREIGN KEY (doctor_id) REFERENCES dim_doctors(doctor_id),
    FOREIGN KEY (treatment_id) REFERENCES dim_treatments(treatment_id)
);
