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
