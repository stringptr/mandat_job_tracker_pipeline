\c postgres

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE silver.adzuna_raw(
  id BIGINT PRIMARY KEY,
  adref TEXT,
  company_display TEXT,
  title TEXT,
  created TIMESTAMP,
  category_tag TEXT,
  category_label TEXT,
  location_area TEXT,
  location_display TEXT,
  longitude DECIMAL,
  latitude DECIMAL,
  salary_max DECIMAL,
  salary_min DECIMAL,
  description TEXT,
  redirect_url TEXT,
  contract_time TEXT,
  contract_type TEXT,
  salary_is_predicted BOOLEAN
);

CREATE TABLE silver.linkedin_raw(
  job_id BIGINT PRIMARY KEY,
  url TEXT,
  title TEXT,
  company TEXT,
  location TEXT,
  posted_at TIMESTAMP
);

SELECT * FROM silver.adzuna_raw WHERE title ILIKE '%data%engineer%';
SELECT * FROM silver.linkedin_raw WHERE title ILIKE '%data%engineer%';
