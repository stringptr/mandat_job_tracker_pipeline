CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE silver.adzuna(
  id BIGINT,
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

CREATE TABLE linkedin(
  job_id BIGINT,
  url TEXT,
  title TEXT,
  company TEXT,
  location TEXT,
  posted_at DATETIME
);

DROP TABLE silver.adzuna;
DROP TABLE silver.linkedin;
DROP SCHEMA silver;
