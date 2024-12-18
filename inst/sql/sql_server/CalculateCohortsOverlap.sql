-- Delete cohorts from cohort table with the cohort_definition_id that have the cohort_names_to_delete in cohort table

SELECT
  cohort_id_combinations,
  COUNT(*) AS number_of_subjects
FROM (
  SELECT
    subject_id,
    STRING_AGG( CAST(CAST(cohort_definition_id AS INT) AS VARCHAR), '-') AS cohort_id_combinations
  FROM (
    SELECT DISTINCT
      cohort_definition_id,
      subject_id
    FROM @cohort_database_schema.@cohort_table
    {@cohort_ids != ''}  ? {WHERE cohort_definition_id IN (@cohort_ids)}
    ORDER BY cohort_definition_id
  ) AS foo
  GROUP BY subject_id
) AS foo
GROUP BY cohort_id_combinations
