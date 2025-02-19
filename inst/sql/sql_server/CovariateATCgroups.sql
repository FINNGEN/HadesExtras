
-- Modified from FeatureExtraction 
-- https://github.com/OHDSI/FeatureExtraction/blob/main/inst/sql/sql_server/DomainConceptGroup.sql

IF OBJECT_ID('tempdb..#atc_groups', 'U') IS NOT NULL
	DROP TABLE #atc_groups;

IF OBJECT_ID('tempdb..#atc_time_period', 'U') IS NOT NULL
	DROP TABLE #atc_time_period;

IF OBJECT_ID('tempdb..#atc_covariate_table', 'U') IS NOT NULL
	DROP TABLE #atc_covariate_table;

IF OBJECT_ID('tempdb..#atc_covariate_ref', 'U') IS NOT NULL
	DROP TABLE #atc_covariate_ref;

IF OBJECT_ID('tempdb..#atc_analysis_ref', 'U') IS NOT NULL
	DROP TABLE #atc_analysis_ref;

-- atc_groups construction
SELECT DISTINCT descendant_concept_id,
  ancestor_concept_id
INTO #atc_groups
FROM @cdm_database_schema.concept_ancestor
INNER JOIN @cdm_database_schema.concept
	ON ancestor_concept_id = concept_id
WHERE vocabulary_id = 'ATC'
	AND LEN(concept_code) IN (1, 3, 4, 5, 7)
	AND concept_id != 0
;

-- atc_time_period construction
CREATE TABLE #atc_time_period (
	time_id INT,
	start_day INT,
	end_day INT
);
INSERT INTO #atc_time_period (
	time_id,
	start_day,
	end_day
)
VALUES 
	@atc_time_period_values
;

-- Feature construction
SELECT 
	CAST(ancestor_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
    time_id,
{@aggregated} ? {
	cohort_definition_id,
	COUNT(*) AS sum_value
} : {
	row_id,
	1 AS covariate_value 
}
INTO #atc_covariate_table
FROM (
	SELECT DISTINCT ancestor_concept_id,
		time_id,
{@aggregated} ? {
		cohort_definition_id,
		cohort.subject_id,
		cohort.cohort_start_date
} : {
		cohort.@row_id_field AS row_id
}	
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.@domain_table
		ON cohort.subject_id = @domain_table.person_id
	INNER JOIN #atc_groups
		ON @domain_concept_id = descendant_concept_id
	INNER JOIN #atc_time_period time_period
		ON @domain_start_date <= DATEADD(DAY, time_period.end_day, cohort.cohort_start_date)
		AND @domain_end_date >= DATEADD(DAY, time_period.start_day, cohort.cohort_start_date)
	WHERE @domain_concept_id != 0

{@cohort_definition_id != -1} ? {		AND cohort.cohort_definition_id IN (@cohort_definition_id)}
) temp
{@aggregated} ? {		
GROUP BY cohort_definition_id,
	ancestor_concept_id
    ,time_id
}
;

TRUNCATE TABLE #atc_groups;
DROP TABLE #atc_groups;
TRUNCATE TABLE #atc_time_period;
DROP TABLE #atc_time_period;

-- Reference constructiond
SELECT covariate_id,
	CAST(CONCAT('@domain_table ATC group: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,
	@analysis_id AS analysis_id,
	CAST((covariate_id - @analysis_id) / 1000 AS INT) AS concept_id
INTO #atc_covariate_ref
FROM (
	SELECT DISTINCT covariate_id
	FROM #atc_covariate_table
	) t1
LEFT JOIN @cdm_database_schema.concept
	ON concept_id = CAST((covariate_id - @analysis_id) / 1000 AS INT);





