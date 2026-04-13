\c postgres

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE gold.junior_job_data_engineer_by_location(
  report_date DATE,
  location_name VARCHAR,
  job_count INT,
  min_salary DECIMAL,
  max_salary DECIMAL,
  avg_salary DECIMAL,
  PRIMARY KEY (report_date, location_name)
);

INSERT INTO gold.junior_job_data_engineer_by_location
SELECT
    CURRENT_DATE AS report_date,
    TRIM(SPLIT_PART(location_display, ',', 1)) AS location_name,
    COUNT(id) AS job_count,
    MIN(salary_min) FILTER (WHERE salary_min > 0) AS min_salary,
    MAX(salary_max) FILTER (WHERE salary_max > 0) AS max_salary,
    AVG((salary_min + salary_max) / 2) FILTER (WHERE salary_min > 0 AND salary_max > 0) AS avg_salary
FROM silver.adzuna_raw
WHERE title ILIKE '%data%engineer%' 
  AND title NOT ILIKE '%senior%' 
  AND title NOT ILIKE '%manager%' 
  AND title NOT ILIKE '%lead%' 
  AND title NOT ILIKE '%head%' 
  AND title NOT ILIKE '%intern%'
  AND title NOT ILIKE '%chief%'
  AND title NOT ILIKE '%president%'
GROUP BY 1, 3
HAVING COUNT(id) > 0
ORDER BY job_count DESC;

SELECT * FROM gold.junior_job_data_engineer_by_location

DROP TABLE gold.junior_job_data_engineer_by_location;
