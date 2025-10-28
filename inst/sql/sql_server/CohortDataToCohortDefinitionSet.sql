-- This code inserts records from an external cohort table into the cohort table of the cohort database schema.
DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (
    cohort_definition_id,
    subject_id,
    cohort_start_date,
    cohort_end_date
)
SELECT 
  cd.cohort_definition_id,
  p.person_id AS subject_id,
  COALESCE(CAST(cd.cohort_start_date AS DATE), op.observation_period_start_date) AS cohort_start_date,
  COALESCE(CAST(cd.cohort_end_date AS DATE), op.observation_period_end_date) AS cohort_end_date
FROM #cohort_data_temp_table AS cd
INNER JOIN @cdm_database_schema.person AS p
  ON cd.person_source_value = p.person_source_value
INNER JOIN (
  SELECT 
    person_id,
    MIN(observation_period_start_date) AS observation_period_start_date,
    MAX(observation_period_end_date) AS observation_period_end_date
  FROM @cdm_database_schema.observation_period AS op
  GROUP BY person_id
) op
  ON p.person_id = op.person_id
WHERE cd.cohort_definition_id = @source_cohort_id
