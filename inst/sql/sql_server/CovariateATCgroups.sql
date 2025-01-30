
-- Modified from FeatureExtraction 
-- https://github.com/OHDSI/FeatureExtraction/blob/main/inst/sql/sql_server/DomainConceptGroup.sql

IF OBJECT_ID('tempdb..#groups', 'U') IS NOT NULL
	DROP TABLE #groups;

SELECT DISTINCT descendant_concept_id,
  ancestor_concept_id
INTO #groups
FROM @cdm_database_schema.concept_ancestor
INNER JOIN @cdm_database_schema.concept
	ON ancestor_concept_id = concept_id
WHERE vocabulary_id = 'ATC'
	AND LEN(concept_code) IN (1, 3, 4, 5, 7)
	AND concept_id != 0
;

-- time_period construction
CREATE TABLE #time_period (
	start_day INT,
	end_day INT
);
INSERT INTO #time_period (
	start_day,
	end_day
)
VALUES (
	@start_day,
	@end_day
);

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
INTO #covariate_table
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
	INNER JOIN #groups
		ON @domain_concept_id = descendant_concept_id
	INNER JOIN #time_period time_period
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
TRUNCATE TABLE #groups;

DROP TABLE #groups;

-- Reference construction
INSERT INTO #cov_ref (
	covariate_id,
	covariate_name,
	analysis_id,
	concept_id
	)
SELECT covariate_id,
	CAST(CONCAT('@domain_table group: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,
	@analysis_id AS analysis_id,
	CAST((covariate_id - @analysis_id) / 1000 AS INT) AS concept_id
FROM (
	SELECT DISTINCT covariate_id
	FROM #covariate_table
	) t1
LEFT JOIN @cdm_database_schema.concept
	ON concept_id = CAST((covariate_id - @analysis_id) / 1000 AS INT);
	
INSERT INTO #analysis_ref (
	analysis_id,
	analysis_name,
	domain_id,
	start_day,
	end_day,
	is_binary,
	missing_means_zero
	)
SELECT @analysis_id AS analysis_id,
	CAST('@analysis_name' AS VARCHAR(512)) AS analysis_name,
	CAST('@domain_id' AS VARCHAR(20)) AS domain_id,
	CAST('Y' AS VARCHAR(1)) AS is_binary,
	CAST(NULL AS VARCHAR(1)) AS missing_means_zero;	
