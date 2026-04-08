CREATE TABLE fact_ems_incident (
    incident_key INT IDENTITY PRIMARY KEY,
    date_key INT,
    patient_key INT,
    location_key INT,
    agency_key INT,
    response_time INT,
    record_count INT DEFAULT 1
);