-- Modified from FeatureExtraction 
-- https://github.com/OHDSI/FeatureExtraction/blob/main/inst/sql/sql_server/DomainConceptGroup.sql

IF OBJECT_ID('tempdb..#pre_computed_covariate_table', 'U') IS NOT NULL
	DROP TABLE #pre_computed_covariate_table;

IF OBJECT_ID('tempdb..#pre_computed_covariate_ref', 'U') IS NOT NULL
	DROP TABLE #pre_computed_covariate_ref;

-- Feature construction
SELECT 
	CAST(ancestor_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
    time_id,
{@aggregated} ? {
	cohort_definition_id,
	COUNT(*) AS count_value,
	MIN(period_ddds) AS min_value,
	MAX(period_ddds) AS max_value,
	AVG(period_ddds) AS average_value,
	STDEV(period_ddds) AS standard_deviation,
	-- TODO: add median, p10, p25, p75, p90
	0 AS median_value,
	0 AS p10_value,
	0 AS p25_value,
	0 AS p75_value,
	0 AS p90_value
} : {
	@row_id_field AS row_id,
	period_ddds AS covariate_value 
}
INTO #atc_ddd_covariate_table
FROM (
	SELECT DISTINCT 
		ancestor_concept_id,
		time_id,
		cohort_definition_id,
		subject_id,
		SUM(event_ddds) AS period_ddds, 
		cohort_start_date
	FROM(
		SELECT DISTINCT 
			atc_ddd_groups.ancestor_concept_id,
			time_period.time_id,
			cohort.cohort_definition_id,
			cohort.subject_id,
			DATEDIFF(DAY, 
				CASE 
					WHEN dt.@domain_start_date > DATEADD(DAY, time_period.start_day, cohort.cohort_start_date) 
					THEN dt.@domain_start_date 
					ELSE DATEADD(DAY, time_period.start_day, cohort.cohort_start_date) 
				END,
				CASE 
					WHEN dt.@domain_end_date < DATEADD(DAY, time_period.end_day, cohort.cohort_start_date) 
					THEN dt.@domain_end_date 
					ELSE DATEADD(DAY, time_period.end_day, cohort.cohort_start_date) 
				END
			) AS event_ddds,
			cohort.cohort_start_date
		FROM @cohort_table cohort
		INNER JOIN @cdm_database_schema.@domain_table dt
			ON cohort.subject_id = dt.person_id
		INNER JOIN #atc_ddd_groups atc_ddd_groups
			ON @domain_concept_id = descendant_concept_id
		INNER JOIN #atc_ddd_time_period time_period
			ON @domain_start_date <= DATEADD(DAY, time_period.end_day, cohort.cohort_start_date)
			AND @domain_end_date >= DATEADD(DAY, time_period.start_day, cohort.cohort_start_date)
		WHERE @domain_concept_id != 0
		{@cohort_definition_id != -1} ? {AND cohort_definition_id IN (@cohort_definition_id)}
	)
	GROUP BY ancestor_concept_id, time_id, cohort_definition_id, subject_id, cohort_start_date
) temp
{@aggregated} ? {		
GROUP BY cohort_definition_id,
	ancestor_concept_id
    ,time_id
}
;

TRUNCATE TABLE #atc_ddd_groups;
DROP TABLE #atc_ddd_groups;
TRUNCATE TABLE #atc_ddd_time_period;
DROP TABLE #atc_ddd_time_period;

-- Reference constructiond
SELECT covariate_id,
	CAST(CONCAT('@domain_table ATC group: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,
	@analysis_id AS analysis_id,
	CAST((covariate_id - @analysis_id) / 1000 AS INT) AS concept_id
INTO #atc_ddd_covariate_ref
FROM (
	SELECT DISTINCT covariate_id
	FROM #atc_ddd_covariate_table
	) t1
LEFT JOIN @cdm_database_schema.concept
	ON concept_id = CAST((covariate_id - @analysis_id) / 1000 AS INT);





