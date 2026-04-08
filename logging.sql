CREATE TABLE etl_log (
    run_id INT IDENTITY PRIMARY KEY,
    start_time DATETIME,
    end_time DATETIME,
    rows_processed INT,
    rows_rejected INT,
    status VARCHAR(20),
    error_message VARCHAR(MAX)
);

CREATE TABLE etl_rejects (
    incident_id INT,
    error_reason VARCHAR(255)
);