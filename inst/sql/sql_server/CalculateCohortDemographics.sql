


SELECT
  cohort_definition_id AS cohort_id,
  YEAR(@reference_year) AS calendar_year,
	FLOOR((YEAR(@reference_year) - year_of_birth) / 10) AS age_group,
	gender_concept_id,
	COUNT(*) AS cohort_count
FROM (
	SELECT *
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@cohort_ids)
	) cohort
INNER JOIN @cdm_database_schema.person
	ON subject_id = person.person_id
GROUP BY
  cohort_definition_id,
  YEAR(@reference_year),
	FLOOR((YEAR(@reference_year) - year_of_birth) / 10),
	gender_concept_id;



