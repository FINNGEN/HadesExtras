-- BEFORE RUN: 1. Needs the insert of the covariate_groups table

-- 2. Gather all patient level data based on the covariate groups and cohort tables
IF OBJECT_ID('tempdb..#pre_computed_cohort', 'U') IS NOT NULL
    DROP TABLE #pre_computed_cohort;

SELECT
    c.cohort_definition_id AS cohort_definition_id,
    ccg.analysis_group_id AS analysis_group_id,
	ccg.concept_class_id AS concept_class_id,
	pcct.concept_id AS concept_id,
	c.subject_id AS subject_id,
	pcct.n_records AS n_records,
	DATEDIFF(DAY, c.cohort_start_date, pcct.first_date) AS day_to_first_event,
	pcct.first_age AS age_first_event,
	pcct.aggregated_value AS aggregated_value,
	c2.concept_code AS unit,
	pcct.aggregated_category AS aggregated_category
INTO #pre_computed_cohort
FROM @cohort_table_schema.@cohort_table_name c
INNER JOIN @results_database_schema.@person_code_counts_table AS pcct ON c.subject_id = pcct.person_id
INNER JOIN #covariate_groups ccg ON pcct.analysis_group = ccg.analysis_group AND pcct.concept_class_id = ccg.concept_class_id
LEFT JOIN @cdm_database_schema.concept c2 ON pcct.aggregated_value_unit = c2.concept_id
{@cohort_definition_id != -1} ? { WHERE c.cohort_definition_id IN (@cohort_definition_id)};


-- 3. Create the pre_computed_covariates table
{@include_binary | @include_categorical} ? {
IF OBJECT_ID('tempdb..#pre_computed_covariates', 'U') IS NOT NULL
	DROP TABLE #pre_computed_covariates;

SELECT *
INTO #pre_computed_covariates 
FROM (
	{@include_binary} ? {
		-- 3.1. Binary
		SELECT
			cohort_definition_id,
			150 + analysis_group_id AS analysis_id,
			concept_id AS concept_id,
			0 AS category_id,
			COUNT(*) AS sum_value
		FROM #pre_computed_cohort
		GROUP BY cohort_definition_id, analysis_group_id, concept_id
	}
	{@include_binary & @include_categorical} ? {
		UNION ALL
	}
	{@include_categorical} ? {
		-- 3.2. Categorical
		SELECT
			cohort_definition_id,
			250 + analysis_group_id AS analysis_id,
			concept_id AS concept_id,
			aggregated_category AS category_id,
			COUNT(*) AS sum_value
		FROM #pre_computed_cohort
		WHERE aggregated_category IS NOT NULL 
		GROUP BY cohort_definition_id, analysis_group_id, concept_id, aggregated_category
	}
);
}

-- 4. Create the pre_computed_covariates_continuous table
{@include_counts | @include_age_first_event | @include_days_to_first_event | @include_continuous} ? {
IF OBJECT_ID('tempdb..#pre_computed_covariates_continuous', 'U') IS NOT NULL
	DROP TABLE #pre_computed_covariates_continuous;

WITH
	{@include_counts} ? {
    -- 4.1. Counts
	ranked_n_records AS (
		SELECT *,
		PERCENT_RANK() OVER (PARTITION BY cohort_definition_id, analysis_group_id, concept_id ORDER BY n_records) AS pct_rank
		FROM #pre_computed_cohort
	)
	}
	{@include_age_first_event & @include_counts} ? { , } 
	{@include_age_first_event} ? {
	-- 4.2. Age First Event
	ranked_age_first_event AS (
		SELECT *,
		PERCENT_RANK() OVER (PARTITION BY cohort_definition_id, analysis_group_id, concept_id ORDER BY age_first_event) AS pct_rank
		FROM #pre_computed_cohort
	)
	}
	{@include_days_to_first_event & (@include_counts | @include_age_first_event)} ? { , }
	{@include_days_to_first_event} ? {
	-- 4.3. Days To First Event
	ranked_days_to_first_event AS (
		SELECT *,
		PERCENT_RANK() OVER (PARTITION BY cohort_definition_id, analysis_group_id, concept_id ORDER BY day_to_first_event) AS pct_rank
		FROM #pre_computed_cohort
	)
	}
	{@include_continuous & (@include_counts | @include_age_first_event | @include_days_to_first_event)} ? { , }
	{@include_continuous} ? {
	-- 4.4. Continuous (Aggregated Value)
	ranked_aggregated_value AS (
		SELECT *,
		PERCENT_RANK() OVER (PARTITION BY cohort_definition_id, analysis_group_id, concept_id ORDER BY aggregated_value) AS pct_rank
		FROM #pre_computed_cohort
		WHERE aggregated_value IS NOT NULL
	)
	}
SELECT *
INTO #pre_computed_covariates_continuous
FROM (
	{@include_counts} ? {
		-- 4.1. Counts		
		SELECT 
		    cohort_definition_id,
			350 + analysis_group_id AS analysis_id,
			concept_id AS concept_id, 
			COUNT(*) AS count_value,
			MIN(n_records) AS min_value,
			MAX(n_records) AS max_value,
			AVG(n_records) AS average_value,
			STDEV(n_records) AS standard_deviation,
			MIN(CASE WHEN pct_rank >= 0.50 THEN n_records END) AS median_value,
			MIN(CASE WHEN pct_rank >= 0.10 THEN n_records END) AS p10_value,
			MIN(CASE WHEN pct_rank >= 0.25 THEN n_records END) AS p25_value,
			MIN(CASE WHEN pct_rank >= 0.75 THEN n_records END) AS p75_value,
			MIN(CASE WHEN pct_rank >= 0.90 THEN n_records END) AS p90_value,
			'counts' AS unit
		FROM ranked_n_records
		GROUP BY cohort_definition_id, analysis_group_id, concept_id, unit
	}
	{@include_age_first_event & @include_counts} ? {
		UNION ALL
	}
	{@include_age_first_event} ? {
		-- 4.2. Age First Event
		SELECT 
		    cohort_definition_id,
			450 + analysis_group_id AS analysis_id,
			concept_id AS concept_id, 
			COUNT(*) AS count_value,
			MIN(age_first_event) AS min_value,
			MAX(age_first_event) AS max_value,
			AVG(age_first_event) AS average_value,
			STDEV(age_first_event) AS standard_deviation,
			MIN(CASE WHEN pct_rank >= 0.50 THEN age_first_event END) AS median_value,
			MIN(CASE WHEN pct_rank >= 0.10 THEN age_first_event END) AS p10_value,
			MIN(CASE WHEN pct_rank >= 0.25 THEN age_first_event END) AS p25_value,
			MIN(CASE WHEN pct_rank >= 0.75 THEN age_first_event END) AS p75_value,
			MIN(CASE WHEN pct_rank >= 0.90 THEN age_first_event END) AS p90_value,
			'years' AS unit
		FROM ranked_age_first_event
		GROUP BY cohort_definition_id, analysis_group_id, concept_id, unit
	}
	{@include_days_to_first_event & (@include_counts | @include_age_first_event)} ? {
		UNION ALL
	}
	{@include_days_to_first_event} ? {
		-- 4.3. Days To First Event
		SELECT 
		    cohort_definition_id,
			550 + analysis_group_id AS analysis_id,
			concept_id AS concept_id, 
			COUNT(*) AS count_value,
			MIN(day_to_first_event) AS min_value,
			MAX(day_to_first_event) AS max_value,
			AVG(day_to_first_event) AS average_value,
			STDEV(day_to_first_event) AS standard_deviation,
			MIN(CASE WHEN pct_rank >= 0.50 THEN day_to_first_event END) AS median_value,
			MIN(CASE WHEN pct_rank >= 0.10 THEN day_to_first_event END) AS p10_value,
			MIN(CASE WHEN pct_rank >= 0.25 THEN day_to_first_event END) AS p25_value,
			MIN(CASE WHEN pct_rank >= 0.75 THEN day_to_first_event END) AS p75_value,
			MIN(CASE WHEN pct_rank >= 0.90 THEN day_to_first_event END) AS p90_value,
			'days' AS unit
		FROM ranked_days_to_first_event
		GROUP BY cohort_definition_id, analysis_group_id, concept_id, unit
	}
	{@include_continuous & (@include_counts | @include_age_first_event | @include_days_to_first_event)} ? {
		UNION ALL
	}
	{@include_continuous} ? {
		-- 4.4. Continuous (Aggregated Value)
		SELECT 
		    cohort_definition_id,
			650 + analysis_group_id AS analysis_id,
			concept_id AS concept_id, 
			COUNT(*) AS count_value,
			MIN(aggregated_value) AS min_value,
			MAX(aggregated_value) AS max_value,
			AVG(aggregated_value) AS average_value,
			STDEV(aggregated_value) AS standard_deviation,
			MIN(CASE WHEN pct_rank >= 0.50 THEN aggregated_value END) AS median_value,
			MIN(CASE WHEN pct_rank >= 0.10 THEN aggregated_value END) AS p10_value,
			MIN(CASE WHEN pct_rank >= 0.25 THEN aggregated_value END) AS p25_value,
			MIN(CASE WHEN pct_rank >= 0.75 THEN aggregated_value END) AS p75_value,
			MIN(CASE WHEN pct_rank >= 0.90 THEN aggregated_value END) AS p90_value,
			unit AS unit
		FROM ranked_aggregated_value
		GROUP BY cohort_definition_id, analysis_group_id, concept_id, unit
	}
);
}


-- 5. Create the pre_computed_concept_ref table
IF OBJECT_ID('tempdb..#pre_computed_concept_ref', 'U') IS NOT NULL
	DROP TABLE #pre_computed_concept_ref;

SELECT DISTINCT
	pct.concept_id AS concept_id,
	c.vocabulary_id AS vocabulary_id,
	c.concept_class_id AS concept_class_id,
	c.concept_code AS concept_code,
	c.concept_name AS concept_name
INTO #pre_computed_concept_ref
FROM (
	SELECT DISTINCT concept_id
	FROM #pre_computed_cohort
) AS pct
LEFT JOIN @cdm_database_schema.concept AS c
ON c.concept_id = pct.concept_id;
