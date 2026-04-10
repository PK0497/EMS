IF OBJECT_ID('dw.fact_ems_incident', 'U') IS NULL
BEGIN
    CREATE TABLE dw.fact_ems_incident (
        incident_key BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        incident_id NVARCHAR(100) NOT NULL,
        date_key INT NOT NULL,
        patient_key INT NOT NULL,
        location_key INT NOT NULL,
        agency_key INT NOT NULL,
        response_time INT NOT NULL,
        record_count INT NOT NULL DEFAULT 1,
        load_run_id BIGINT NOT NULL,
        load_time DATETIME2(0) NOT NULL,
        CONSTRAINT UQ_fact_ems_incident UNIQUE (incident_id),
        CONSTRAINT FK_fact_date FOREIGN KEY (date_key) REFERENCES dw.dim_date(date_key),
        CONSTRAINT FK_fact_patient FOREIGN KEY (patient_key) REFERENCES dw.dim_patient(patient_key),
        CONSTRAINT FK_fact_location FOREIGN KEY (location_key) REFERENCES dw.dim_location(location_key),
        CONSTRAINT FK_fact_agency FOREIGN KEY (agency_key) REFERENCES dw.dim_agency(agency_key)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fact_ems_incident_date' AND object_id = OBJECT_ID('dw.fact_ems_incident'))
    CREATE INDEX IX_fact_ems_incident_date ON dw.fact_ems_incident(date_key);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fact_ems_incident_patient' AND object_id = OBJECT_ID('dw.fact_ems_incident'))
    CREATE INDEX IX_fact_ems_incident_patient ON dw.fact_ems_incident(patient_key);
GO