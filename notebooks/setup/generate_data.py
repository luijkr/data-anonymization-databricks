# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generation
# MAGIC This notebook generates synthetic data for the healthcare data model. The data is generated using the Faker library and saved to tables in the Databricks catalog.

# COMMAND ----------
# DBTITLE 1,Install Faker library
# MAGIC %sh pip install Faker

# COMMAND ----------
# DBTITLE 1,Import libraries
import random
import uuid

from faker import Faker
from pyspark.sql import types as T

fake = Faker()
Faker.seed(42)

# COMMAND ----------
# DBTITLE 1,Configuration

# Retrieve Databricks widget values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Table Names
patients_table = f"{catalog_name}.{schema_name}.dim_patients"
hospitals_table = f"{catalog_name}.{schema_name}.dim_hospitals"
doctors_table = f"{catalog_name}.{schema_name}.dim_doctors"
fact_visits_table = f"{catalog_name}.{schema_name}.fact_visits"

# Number of records to generate
num_patients = 1000
num_hospitals = 50
num_doctors = 200
num_fact_visits = 5000

# COMMAND ----------
# DBTITLE 1,Define functions to generate data
def generate_patients(num_patients: int) -> tuple[int, str, str, str, str]:
    """
    Generate patient data.

    Columns generated:
    - patient_id: int
    - name: str
    - dob: str
    - ssn: str
    - address: str
    """
    for i in range(num_patients):
        yield (
            i,
            fake.name(),
            fake.date_of_birth(minimum_age=20, maximum_age=90),
            fake.ssn(),
            fake.address(),
        )


def generate_hospitals(num_hospitals: int) -> tuple[int, str, str, str]:
    """
    Generate hospital data.

    Columns generated:
    - hospital_id: int
    - name: str
    - address: str
    - phone: str
    """
    for i in range(num_hospitals):
        yield (i, fake.company(), fake.address(), fake.phone_number())


def generate_doctors(num_doctors: int) -> tuple[int, str, str, int]:
    """
    Generate doctor data.

    Columns generated:
    - doctor_id: int
    - name: str
    - specialization: str
    - hospital_id: int
    """
    for i in range(num_doctors):
        yield (i, fake.name(), fake.job(), random.randint(1, 50))


def generate_visits(num_visits: int) -> tuple[int, int, int, str]:
    """
    Generate visit data.

    Columns generated:
    - visit_id: int
    - patient_id: int
    - hospital_id: int
    - visit_date: str
    """
    for i in range(num_visits):
        yield (i, random.randint(1, 1000), random.randint(1, 50), fake.date_this_decade())

# COMMAND ----------
# DBTITLE 1,Define schemas
schema_patients = T.StructType(
    [
        T.StructField("patient_id", T.IntegerType(), False),
        T.StructField("name", T.StringType(), False),
        T.StructField("dob", T.DateType(), False),
        T.StructField("ssn", T.StringType(), False),
        T.StructField("address", T.StringType(), False),
    ]
)

schema_hospitals = T.StructType(
    [
        T.StructField("hospital_id", T.IntegerType(), False),
        T.StructField("name", T.StringType(), False),
        T.StructField("address", T.StringType(), False),
        T.StructField("phone", T.StringType(), False),
    ]
)

schema_doctors = T.StructType(
    [
        T.StructField("doctor_id", T.IntegerType(), False),
        T.StructField("name", T.StringType(), False),
        T.StructField("specialization", T.StringType(), False),
        T.StructField("hospital_id", T.IntegerType(), False),
    ]
)

schema_visits = T.StructType(
    [
        T.StructField("visit_id", T.IntegerType(), False),
        T.StructField("patient_id", T.IntegerType(), False),
        T.StructField("hospital_id", T.IntegerType(), False),
        T.StructField("visit_date", T.DateType(), False),
    ]
)

# COMMAND ----------
# DBTITLE 1,Generate data
patients_dim = spark.createDataFrame(generate_patients(num_patients), schema=schema_patients)
hospitals_dim = spark.createDataFrame(generate_hospitals(num_hospitals), schema=schema_hospitals)
doctors_dim = spark.createDataFrame(generate_doctors(num_doctors), schema=schema_doctors)
visits_fact = spark.createDataFrame(generate_visits(num_fact_visits), schema=schema_visits)

# COMMAND ----------
# DBTITLE 1,Save data to tables
patients_dim.write.mode("overwrite").saveAsTable(patients_table)
hospitals_dim.write.mode("overwrite").saveAsTable(hospitals_table)
doctors_dim.write.mode("overwrite").saveAsTable(doctors_table)
visits_fact.write.mode("overwrite").saveAsTable(fact_visits_table)
