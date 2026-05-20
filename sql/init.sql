-- PostgreSQL initialization for Spark Streaming course

\c rtjvm;

-- Table for the JDBC integration lesson (Part 3.2)
CREATE TABLE IF NOT EXISTS public.cars (
  "Name" VARCHAR(100),
  "Miles_per_Gallon" DOUBLE PRECISION,
  "Cylinders" BIGINT,
  "Displacement" DOUBLE PRECISION,
  "Horsepower" BIGINT,
  "Weight_in_lbs" BIGINT,
  "Acceleration" DOUBLE PRECISION,
  "Year" VARCHAR(20),
  "Origin" VARCHAR(20)
);

-- Table for the GitHub Events capstone project (Part 5.2)
CREATE TABLE IF NOT EXISTS public.github_events (
  id VARCHAR(100),
  "eventType" VARCHAR(50),
  "actorLogin" VARCHAR(100),
  "repoName" VARCHAR(200),
  "createdAt" TIMESTAMP
);
