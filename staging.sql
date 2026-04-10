IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

IF OBJECT_ID('stg.ems_raw', 'U') IS NULL
BEGIN
    CREATE TABLE stg.ems_raw (
        stg_raw_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        run_id BIGINT NOT NULL,
        source_system NVARCHAR(100) NOT NULL,
        source_row_number INT NOT NULL,
        incident_id_raw NVARCHAR(100) NULL,
        patient_id_raw NVARCHAR(100) NULL,
        age_raw NVARCHAR(50) NULL,
        gender_raw NVARCHAR(50) NULL,
        incident_date_raw NVARCHAR(50) NULL,
        city_raw NVARCHAR(200) NULL,
        agency_raw NVARCHAR(200) NULL,
        response_time_raw NVARCHAR(50) NULL,
        unicode_notes NVARCHAR(255) NULL,
        loaded_at DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID('stg.ems_valid', 'U') IS NULL
BEGIN
    CREATE TABLE stg.ems_valid (
        stg_valid_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        run_id BIGINT NOT NULL,
        source_row_number INT NOT NULL,
        incident_id NVARCHAR(100) NOT NULL,
        patient_id NVARCHAR(100) NOT NULL,
        age INT NOT NULL,
        gender NVARCHAR(20) NOT NULL,
        incident_date DATE NOT NULL,
        date_key INT NOT NULL,
        city NVARCHAR(200) NOT NULL,
        agency NVARCHAR(200) NOT NULL,
        response_time INT NOT NULL,
        loaded_at DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_stg_ems_raw_run' AND object_id = OBJECT_ID('stg.ems_raw'))
    CREATE INDEX IX_stg_ems_raw_run ON stg.ems_raw(run_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_stg_ems_valid_run' AND object_id = OBJECT_ID('stg.ems_valid'))
    CREATE INDEX IX_stg_ems_valid_run ON stg.ems_valid(run_id);
GO