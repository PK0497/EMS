IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw');
GO

IF OBJECT_ID('dw.dim_date', 'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_date (
        date_key INT NOT NULL PRIMARY KEY,
        full_date DATE NOT NULL,
        [year] INT NOT NULL,
        [month] INT NOT NULL,
        [day] INT NOT NULL
    );
END;
GO

IF OBJECT_ID('dw.dim_patient', 'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_patient (
        patient_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        patient_id NVARCHAR(100) NOT NULL,
        gender NVARCHAR(20) NOT NULL,
        age INT NOT NULL,
        start_date DATETIME2(0) NOT NULL,
        end_date DATETIME2(0) NOT NULL,
        is_current BIT NOT NULL,
        CONSTRAINT UQ_dim_patient_version UNIQUE(patient_id, start_date)
    );
END;
GO

IF OBJECT_ID('dw.dim_location', 'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_location (
        location_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        city NVARCHAR(200) NOT NULL UNIQUE
    );
END;
GO

IF OBJECT_ID('dw.dim_agency', 'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_agency (
        agency_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        agency_name NVARCHAR(200) NOT NULL UNIQUE
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dim_patient_lookup' AND object_id = OBJECT_ID('dw.dim_patient'))
    CREATE INDEX IX_dim_patient_lookup ON dw.dim_patient(patient_id, is_current);
GO

-- Unknown/default members required for robust referential integrity.
SET IDENTITY_INSERT dw.dim_patient ON;
IF NOT EXISTS (SELECT 1 FROM dw.dim_patient WHERE patient_key = 0)
    INSERT INTO dw.dim_patient(patient_key, patient_id, gender, age, start_date, end_date, is_current)
    VALUES (0, 'UNKNOWN', 'UNKNOWN', 0, '19000101', '99991231', 1);
SET IDENTITY_INSERT dw.dim_patient OFF;
GO

SET IDENTITY_INSERT dw.dim_location ON;
IF NOT EXISTS (SELECT 1 FROM dw.dim_location WHERE location_key = 0)
    INSERT INTO dw.dim_location(location_key, city) VALUES (0, 'UNKNOWN');
SET IDENTITY_INSERT dw.dim_location OFF;
GO

SET IDENTITY_INSERT dw.dim_agency ON;
IF NOT EXISTS (SELECT 1 FROM dw.dim_agency WHERE agency_key = 0)
    INSERT INTO dw.dim_agency(agency_key, agency_name) VALUES (0, 'UNKNOWN');
SET IDENTITY_INSERT dw.dim_agency OFF;
GO

IF NOT EXISTS (SELECT 1 FROM dw.dim_date WHERE date_key = 0)
    INSERT INTO dw.dim_date(date_key, full_date, [year], [month], [day])
    VALUES (0, '19000101', 1900, 1, 1);
GO