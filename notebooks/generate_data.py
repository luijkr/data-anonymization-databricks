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

from faker import Faker
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

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
treatments_table = f"{catalog_name}.{schema_name}.dim_treatments"
fact_treatments_table = f"{catalog_name}.{schema_name}.fact_treatments"

# Number of records to generate
num_patients = 1000
num_hospitals = 50
num_doctors = 200
num_fact_visits = 5000
num_treatments = 100
num_fact_treatments = 10000


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
    for i in range(1, num_patients + 1):
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
    for i in range(1, num_hospitals + 1):
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
    for i in range(1, num_doctors + 1):
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
    for i in range(1, num_visits + 1):
        yield (i, random.randint(1, 1000), random.randint(1, 50), fake.date_this_decade())


def generate_treatments(num_treatments: int) -> tuple[int, str, float]:
    """
    Generate treatment data.

    Columns generated:
    - treatment_id: int
    - treatment_name: str
    - cost: float
    """
    treatments_options = [
        "MRI Scan",
        "X-ray",
        "Blood Test",
        "CT Scan",
        "Physical Therapy",
        "Surgery",
    ]
    for i in range(1, num_treatments + 1):
        yield (i, random.choice(treatments_options), round(random.uniform(100, 5000), 2))


def generate_fact_treatments(num_treatments: int) -> tuple[int, int, int, int, int, str]:
    """
    Generate fact treatment data.

    Columns generated:
    - treatment_record_id: int
    - patient_id: int
    - visit_id: int
    - doctor_id: int
    - treatment_id: int
    - treatment_date: str
    """
    for i in range(1, num_treatments + 1):
        yield (
            i,
            random.randint(1, 1000),  # patient_id
            random.randint(1, 5000),  # visit_id
            random.randint(1, 200),  # doctor_id
            random.randint(1, 100),  # treatment_id
            fake.date_this_decade(),
        )


# COMMAND ----------
# DBTITLE 1,Define schemas
schema_patients = StructType(
    [
        StructField("patient_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("dob", DateType(), False),
        StructField("ssn", StringType(), False),
        StructField("address", StringType(), False),
    ]
)

schema_hospitals = StructType(
    [
        StructField("hospital_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("address", StringType(), False),
        StructField("phone", StringType(), False),
    ]
)

schema_doctors = StructType(
    [
        StructField("doctor_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("specialization", StringType(), False),
        StructField("hospital_id", IntegerType(), False),
    ]
)

schema_treatments = StructType(
    [
        StructField("treatment_id", IntegerType(), False),
        StructField("treatment_name", StringType(), False),
        StructField("cost", DoubleType(), False),
    ]
)

schema_fact_treatments = StructType(
    [
        StructField("treatment_record_id", IntegerType(), False),
        StructField("patient_id", IntegerType(), False),
        StructField("visit_id", IntegerType(), False),
        StructField("doctor_id", IntegerType(), False),
        StructField("treatment_id", IntegerType(), False),
        StructField("treatment_date", DateType(), False),
    ]
)

schema_visits = StructType(
    [
        StructField("visit_id", IntegerType(), False),
        StructField("patient_id", IntegerType(), False),
        StructField("hospital_id", IntegerType(), False),
        StructField("visit_date", DateType(), False),
    ]
)

# COMMAND ----------
# DBTITLE 1,Generate data
patients_dim = spark.createDataFrame(generate_patients(num_patients), schema=schema_patients)
hospitals_dim = spark.createDataFrame(generate_hospitals(num_hospitals), schema=schema_hospitals)
doctors_dim = spark.createDataFrame(generate_doctors(num_doctors), schema=schema_doctors)
visits_fact = spark.createDataFrame(generate_visits(num_fact_visits), schema=schema_visits)
treatments_dim = spark.createDataFrame(
    generate_treatments(num_treatments), schema=schema_treatments
)
treatments_fact = spark.createDataFrame(
    generate_fact_treatments(num_fact_treatments), schema=schema_fact_treatments
)

# COMMAND ----------
# DBTITLE 1,Save data to tables
patients_dim.write.mode("overwrite").saveAsTable(patients_table)
hospitals_dim.write.mode("overwrite").saveAsTable(hospitals_table)
doctors_dim.write.mode("overwrite").saveAsTable(doctors_table)
visits_fact.write.mode("overwrite").saveAsTable(fact_visits_table)
treatments_dim.write.mode("overwrite").saveAsTable(treatments_table)
treatments_fact.write.mode("overwrite").saveAsTable(fact_treatments_table)
