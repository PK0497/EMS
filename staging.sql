CREATE TABLE stg_ems_raw (
    incident_id INT,
    patient_id VARCHAR(50),
    age INT,
    gender VARCHAR(10),
    incident_date DATE,
    city VARCHAR(50),
    agency VARCHAR(50),
    response_time INT,
    unicode_notes NVARCHAR(255)
);