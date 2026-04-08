CREATE TABLE dim_patient (
    patient_key INT IDENTITY PRIMARY KEY,
    patient_id VARCHAR(50),
    gender VARCHAR(10),
    age INT,
    start_date DATETIME,
    end_date DATETIME,
    is_current BIT
);

CREATE TABLE dim_location (
    location_key INT IDENTITY PRIMARY KEY,
    city VARCHAR(50)
);

CREATE TABLE dim_agency (
    agency_key INT IDENTITY PRIMARY KEY,
    agency_name VARCHAR(50)
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT
);