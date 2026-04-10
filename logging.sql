IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ctl')
    EXEC('CREATE SCHEMA ctl');
GO

IF OBJECT_ID('ctl.etl_run_log', 'U') IS NULL
BEGIN
    CREATE TABLE ctl.etl_run_log (
        run_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        start_time DATETIME2(0) NOT NULL,
        end_time DATETIME2(0) NULL,
        status NVARCHAR(20) NOT NULL,
        load_mode NVARCHAR(20) NOT NULL,
        source_system NVARCHAR(100) NOT NULL,
        rows_extracted INT NOT NULL DEFAULT 0,
        rows_staged_raw INT NOT NULL DEFAULT 0,
        rows_valid INT NOT NULL DEFAULT 0,
        rows_rejected INT NOT NULL DEFAULT 0,
        rows_dim_inserted INT NOT NULL DEFAULT 0,
        rows_dim_updated INT NOT NULL DEFAULT 0,
        rows_fact_inserted INT NOT NULL DEFAULT 0,
        rows_fact_updated INT NOT NULL DEFAULT 0,
        error_message NVARCHAR(MAX) NULL
    );
END;
GO

IF OBJECT_ID('ctl.etl_step_log', 'U') IS NULL
BEGIN
    CREATE TABLE ctl.etl_step_log (
        step_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        run_id BIGINT NOT NULL,
        step_name NVARCHAR(200) NOT NULL,
        start_time DATETIME2(0) NOT NULL,
        end_time DATETIME2(0) NULL,
        status NVARCHAR(20) NOT NULL,
        rows_affected INT NOT NULL DEFAULT 0,
        error_message NVARCHAR(MAX) NULL,
        CONSTRAINT FK_etl_step_run FOREIGN KEY (run_id) REFERENCES ctl.etl_run_log(run_id)
    );
END;
GO

IF OBJECT_ID('ctl.etl_rejects', 'U') IS NULL
BEGIN
    CREATE TABLE ctl.etl_rejects (
        reject_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        run_id BIGINT NOT NULL,
        incident_id NVARCHAR(100) NULL,
        source_row_number INT NULL,
        error_type NVARCHAR(100) NOT NULL,
        error_message NVARCHAR(1000) NOT NULL,
        created_at DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_etl_rejects_run FOREIGN KEY (run_id) REFERENCES ctl.etl_run_log(run_id)
    );
END;
GO

IF OBJECT_ID('ctl.watermark', 'U') IS NULL
BEGIN
    CREATE TABLE ctl.watermark (
        entity_name NVARCHAR(100) PRIMARY KEY,
        watermark_value NVARCHAR(100) NOT NULL,
        updated_at DATETIME2(0) NOT NULL
    );
END;
GO