-- Get Cohort Demographics SQL Template
-- This template uses sqlRender conditional logic to include different demographic features

WITH demographics_data AS (
  -- Cohort Counts (entries)
  {@cohortEntries} ? {
    SELECT 
      cohort_definition_id as cohort_id,
      'cohortEntries' as feature,
      'total' as bin,
      COUNT(*) as counts
    FROM @cohort_database_schema.@cohort_table
    {@cohort_ids != ''} ? {WHERE cohort_definition_id IN (@cohort_ids)}
    GROUP BY cohort_definition_id
    
    UNION ALL
  }
  
  -- Cohort Counts (subjects)
  {@cohortSubjects} ? {
    SELECT 
      cohort_definition_id as cohort_id,
      'cohortSubjects' as feature,
      'total' as bin,
      COUNT(DISTINCT subject_id) as counts
    FROM @cohort_database_schema.@cohort_table
    {@cohort_ids != ''} ? {WHERE cohort_definition_id IN (@cohort_ids)}
    GROUP BY cohort_definition_id
    
    UNION ALL
  }
  
  {@histogramCohortStartYear} ? {
    SELECT 
      cohort_definition_id as cohort_id,
      'histogramCohortStartYear' as feature,
      CAST(YEAR(cohort_start_date) AS VARCHAR(10)) as bin,
      COUNT(*) as counts
    FROM @cohort_database_schema.@cohort_table
    {@cohort_ids != ''} ? {WHERE cohort_definition_id IN (@cohort_ids)}
    GROUP BY cohort_definition_id, YEAR(cohort_start_date)
    
    UNION ALL
  }
  
  {@histogramCohortEndYear} ? {
    SELECT 
      cohort_definition_id as cohort_id,
      'histogramCohortEndYear' as feature,
      CAST(YEAR(cohort_end_date) AS VARCHAR(10)) as bin,
      COUNT(*) as counts
    FROM @cohort_database_schema.@cohort_table
    {@cohort_ids != ''} ? {WHERE cohort_definition_id IN (@cohort_ids)}
    GROUP BY cohort_definition_id, YEAR(cohort_end_date)
    
    UNION ALL
  }
  
  {@histogramBirthYear} ? {
    SELECT 
      c.cohort_definition_id as cohort_id,
      'histogramBirthYear' as feature,
      CAST(p.year_of_birth AS VARCHAR(10)) as bin,
      COUNT(DISTINCT c.subject_id) as counts
    FROM @cohort_database_schema.@cohort_table c
    INNER JOIN @cdm_database_schema.person p
      ON c.subject_id = p.person_id
    {@cohort_ids != ''} ? {WHERE c.cohort_definition_id IN (@cohort_ids)}
    GROUP BY c.cohort_definition_id, p.year_of_birth
    
    UNION ALL
  }
  
  {@histogramBirthYearAllEvents} ? {
    SELECT 
      c.cohort_definition_id as cohort_id,
      'histogramBirthYearAllEvents' as feature,
      CAST(p.year_of_birth AS VARCHAR(10)) as bin,
      COUNT(*) as counts
    FROM @cohort_database_schema.@cohort_table c
    INNER JOIN @cdm_database_schema.person p
      ON c.subject_id = p.person_id
    {@cohort_ids != ''} ? {WHERE c.cohort_definition_id IN (@cohort_ids)}
    GROUP BY c.cohort_definition_id, p.year_of_birth
    
    UNION ALL
  }
  
  {@sexCounts} ? {
    SELECT 
      c.cohort_definition_id as cohort_id,
      'sexCounts' as feature,
      COALESCE(concept_name, 'Unknown') as bin,
      COUNT(DISTINCT c.subject_id) as counts
    FROM @cohort_database_schema.@cohort_table c
    INNER JOIN @cdm_database_schema.person p
      ON c.subject_id = p.person_id
    LEFT JOIN @vocabulary_database_schema.concept concept
      ON p.gender_concept_id = concept.concept_id
    {@cohort_ids != ''} ? {WHERE c.cohort_definition_id IN (@cohort_ids)}
    GROUP BY c.cohort_definition_id, concept_name
    
    UNION ALL
  }
  
  {@sexCountsAllEvents} ? {
    SELECT 
      c.cohort_definition_id as cohort_id,
      'sexCountsAllEvents' as feature,
      COALESCE(concept_name, 'Unknown') as bin,
      COUNT(*) as counts
    FROM @cohort_database_schema.@cohort_table c
    INNER JOIN @cdm_database_schema.person p
      ON c.subject_id = p.person_id
    LEFT JOIN @vocabulary_database_schema.concept concept
      ON p.gender_concept_id = concept.concept_id
    {@cohort_ids != ''} ? {WHERE c.cohort_definition_id IN (@cohort_ids)}
    GROUP BY c.cohort_definition_id, concept_name
    
    UNION ALL
  }
  
  -- Empty UNION ALL to avoid syntax errors when no blocks are included
  SELECT 
    NULL as cohort_id,
    NULL as feature,
    NULL as bin,
    NULL as counts
  WHERE 1 = 0
)
SELECT 
  cohort_id,
  feature,
  bin,
  counts
FROM demographics_data
WHERE cohort_id IS NOT NULL
ORDER BY cohort_id, feature, bin;
