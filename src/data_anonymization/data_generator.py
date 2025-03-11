import random
from datetime import datetime

from faker import Faker
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

fake = Faker()
Faker.seed(42)


def generate_patients(num_patients: int):
    for i in range(1, num_patients + 1):
        yield (
            i,
            fake.name(),
            fake.date_of_birth(minimum_age=20, maximum_age=90),
            fake.ssn(),
            fake.address(),
        )


schema_patients = StructType(
    [
        StructField("patient_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("dob", DateType(), False),
        StructField("ssn", StringType(), False),
        StructField("address", StringType(), False),
    ]
)


def generate_hospitals(num_hospitals: int):
    for i in range(1, num_hospitals + 1):
        yield (i, fake.company(), fake.address(), fake.phone_number())


schema_hospitals = StructType(
    [
        StructField("hospital_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("address", StringType(), False),
        StructField("phone", StringType(), False),
    ]
)


def generate_doctors(num_doctors: int):
    for i in range(1, num_doctors + 1):
        yield (i, fake.name(), fake.job(), random.randint(1, 50))


schema_doctors = StructType(
    [
        StructField("doctor_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("specialization", StringType(), False),
        StructField("hospital_id", IntegerType(), False),
    ]
)


def generate_visits(num_visits: int):
    for i in range(1, num_visits + 1):
        yield (i, random.randint(1, 1000), random.randint(1, 50), fake.date_this_decade())


schema_visits = StructType(
    [
        StructField("visit_id", IntegerType(), False),
        StructField("patient_id", IntegerType(), False),
        StructField("hospital_id", IntegerType(), False),
        StructField("visit_date", DateType(), False),
    ]
)


def generate_treatments(num_treatments: int):
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


schema_treatments = StructType(
    [
        StructField("treatment_id", IntegerType(), False),
        StructField("treatment_name", StringType(), False),
        StructField("cost", DoubleType(), False),
    ]
)


def generate_fact_treatments(num_treatments: int):
    for i in range(1, num_treatments + 1):
        yield (
            i,
            random.randint(1, 1000),  # patient_id
            random.randint(1, 5000),  # visit_id
            random.randint(1, 200),  # doctor_id
            random.randint(1, 100),  # treatment_id
            fake.date_this_decade(),
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
