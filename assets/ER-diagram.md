```mermaid
erDiagram
    PATIENTS_DIM {
        int patient_id PK
        string name
        date dob
        string ssn
        string address
    }

    HOSPITALS_DIM {
        int hospital_id PK
        string name
        string address
        string phone
    }

    DOCTORS_DIM {
        int doctor_id PK
        string name
        string specialization
        int hospital_id FK
    }

    VISITS_FACT {
        int visit_id PK
        int patient_id FK
        int hospital_id FK
        date visit_date
    }

    TREATMENTS_DIM {
        int treatment_id PK
        string treatment_name
        double cost
    }

    TREATMENTS_FACT {
        int treatment_record_id PK
        int patient_id FK
        int visit_id FK
        int doctor_id FK
        int treatment_id FK
        date treatment_date
    }

    PATIENTS_DIM ||--o{ VISITS_FACT : "patient has visits"
    HOSPITALS_DIM ||--o{ VISITS_FACT : "hospital receives visits"
    HOSPITALS_DIM ||--o{ DOCTORS_DIM : "hospital employs doctor"
    VISITS_FACT ||--o{ TREATMENTS_FACT : "visit includes treatments"
    PATIENTS_DIM ||--o{ TREATMENTS_FACT : "patient undergoes treatment"
    DOCTORS_DIM ||--o{ TREATMENTS_FACT : "doctor gives treatment"
    TREATMENTS_DIM ||--o{ TREATMENTS_FACT : "applies"
