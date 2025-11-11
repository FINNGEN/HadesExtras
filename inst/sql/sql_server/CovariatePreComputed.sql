-- Modified from FeatureExtraction 
-- https://github.com/OHDSI/FeatureExtraction/blob/main/inst/sql/sql_server/DomainConceptGroup.sql

IF OBJECT_ID('tempdb..#pre_computed_covariate_table', 'U') IS NOT NULL
	DROP TABLE #pre_computed_covariate_table;

IF OBJECT_ID('tempdb..#pre_computed_covariate_ref', 'U') IS NOT NULL
	DROP TABLE #pre_computed_covariate_ref;

-- Feature construction
{@aggregated} ? {
WITH ranked AS (
	SELECT 
		*,
		PERCENT_RANK() OVER (PARTITION BY cohort_definition_id, analysis_group_id, concept_id 
							 ORDER BY n_records) AS pct_rank
	FROM #pre_computed_cohort
)
SELECT 
	CAST(concept_id AS BIGINT) * 1000 + 150 + analysis_group_id AS covariate_id, 
	1 AS time_id,
	cohort_definition_id,
	analysis_group_id,
	concept_id,
	COUNT(*) AS count_value,
	MIN(n_records) AS min_value,
	MAX(n_records) AS max_value,
	AVG(n_records) AS average_value,
	STDEV(n_records) AS standard_deviation,
	MIN(CASE WHEN pct_rank >= 0.50 THEN n_records END) AS median_value,
	MIN(CASE WHEN pct_rank >= 0.10 THEN n_records END) AS p10_value,
	MIN(CASE WHEN pct_rank >= 0.25 THEN n_records END) AS p25_value,
	MIN(CASE WHEN pct_rank >= 0.75 THEN n_records END) AS p75_value,
	MIN(CASE WHEN pct_rank >= 0.90 THEN n_records END) AS p90_value
INTO #pre_computed_covariate_table
FROM ranked
GROUP BY cohort_definition_id, analysis_group_id, concept_class_id, concept_id
} : {
SELECT 
	CAST(concept_id AS BIGINT) * 1000 + 150 + analysis_group_id AS covariate_id,
	analysis_group_id,
	@row_id_field AS row_id,
	n_records AS covariate_value 
INTO #pre_computed_covariate_table
FROM #pre_computed_cohort 
}

-- Covariate Reference construction
SELECT 
	pct.covariate_id AS covariate_id,
	CAST(CONCAT( c.concept_class_id, '|', CASE WHEN c.concept_name IS NULL THEN 'Unknown concept' ELSE c.concept_name END) AS VARCHAR(512)) AS covariate_name,
	CAST((pct.covariate_id) % 1000 AS INT) AS analysis_id,
	CAST((pct.covariate_id) / 1000 AS INT) AS concept_id
INTO #pre_computed_covariate_ref
FROM #pre_computed_covariate_table AS pct
LEFT JOIN @cdm_database_schema.concept AS c
ON c.concept_id = CAST((pct.covariate_id / 1000) AS INT);
