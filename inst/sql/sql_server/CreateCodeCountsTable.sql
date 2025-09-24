-- Create the code counts table
DROP TABLE IF EXISTS @resultsDatabaseSchema.temp_concept_ancestor;
DROP TABLE IF EXISTS @resultsDatabaseSchema.@personCodeCountsTable;

CREATE TABLE @resultsDatabaseSchema.temp_concept_ancestor (
    ancestor_concept_id INTEGER,
    descendant_concept_id INTEGER,
    ancestor_vocabulary_id VARCHAR(255)
);

CREATE TABLE @resultsDatabaseSchema.@personCodeCountsTable (
    analysis_type VARCHAR(255),
    person_id INTEGER,
    concept_id INTEGER,
    n_records INTEGER,
    first_date DATE,
    first_age INTEGER,
    aggregated_value FLOAT,
    aggregated_value_unit INTEGER,
    aggregated_category INTEGER
);

-- Create a temporary concept ancestor table that also includes all the concepts to themselves
INSERT INTO @resultsDatabaseSchema.temp_concept_ancestor (
	ancestor_concept_id,
	descendant_concept_id,
	ancestor_vocabulary_id
	)
SELECT DISTINCT 
    ca.ancestor_concept_id AS ancestor_concept_id,
    ca.descendant_concept_id AS descendant_concept_id,
    c.vocabulary_id AS ancestor_vocabulary_id
 FROM (
    SELECT * FROM @cdmDatabaseSchema.concept_ancestor
    UNION ALL
    SELECT DISTINCT
        concept_id AS ancestor_concept_id,
        concept_id AS descendant_concept_id,
        0 AS min_levels_of_separation,
        0 AS max_levels_of_separation
    FROM
        @cdmDatabaseSchema.concept
) AS ca
LEFT JOIN @cdmDatabaseSchema.concept AS c
ON ca.ancestor_concept_id = c.concept_id;

-- Append standard analysis for Condition, Procedure, Observation, Device, and all ancestors
INSERT INTO @resultsDatabaseSchema.@personCodeCountsTable 
    
SELECT
    pcac.domain_id AS analysis_type,
    pcac.person_id AS person_id,
    tca.ancestor_concept_id AS concept_id,
    SUM(pcac.n_records) AS n_records,
    MIN(pcac.first_date) AS first_date,
    MIN(pcac.first_age) AS first_age,
    NULL AS aggregated_value,
    NULL AS aggregated_value_unit,
    NULL AS aggregated_category

FROM @resultsDatabaseSchema.temp_concept_ancestor AS tca
INNER JOIN @resultsDatabaseSchema.@personCodeAtomicCountsTable AS pcac
    ON tca.descendant_concept_id = pcac.concept_id
WHERE pcac.domain_id != 'Drug' AND pcac.domain_id != 'Measurement'
GROUP BY
    pcac.domain_id,
    tca.ancestor_concept_id,
    pcac.person_id;


-- Append non-standard analysis for Condition, Procedure, Observation, Device, and all ancestors
INSERT INTO @resultsDatabaseSchema.@personCodeCountsTable 
    
SELECT
    tca.ancestor_vocabulary_id AS analysis_type,
    pcac.person_id AS person_id,
    tca.ancestor_concept_id AS concept_id,
    SUM(pcac.n_records) AS n_records,
    MIN(pcac.first_date) AS first_date,
    MIN(pcac.first_age) AS first_age,
    NULL AS aggregated_value,
    NULL AS aggregated_value_unit,
    NULL AS aggregated_category

FROM @resultsDatabaseSchema.temp_concept_ancestor AS tca
INNER JOIN @resultsDatabaseSchema.@personCodeAtomicCountsTable AS pcac
    ON tca.descendant_concept_id = pcac.maps_to_concept_id
WHERE pcac.domain_id != 'Drug' AND pcac.domain_id != 'Measurement'
GROUP BY
    tca.ancestor_vocabulary_id,
    tca.ancestor_concept_id,
    pcac.person_id;

-- Append ATC levels drugs  
INSERT INTO @resultsDatabaseSchema.@personCodeCountsTable 
    
SELECT
    'ATC' AS analysis_type,
    pcac.person_id AS person_id,
    tca.ancestor_concept_id AS concept_id,
    SUM(pcac.n_records) AS n_records,
    MIN(pcac.first_date) AS first_date,
    MIN(pcac.first_age) AS first_age,
    SUM(pcac.aggregated_value) AS aggregated_value,
    8512 AS aggregated_value_unit,
    NULL AS aggregated_category

FROM ( SELECT * FROM @resultsDatabaseSchema.temp_concept_ancestor WHERE ancestor_vocabulary_id = 'ATC') AS tca
INNER JOIN @resultsDatabaseSchema.@personCodeAtomicCountsTable AS pcac
    ON tca.descendant_concept_id = pcac.concept_id
GROUP BY
    tca.ancestor_concept_id,
    pcac.person_id;

-- Append Measurements, only standard analysis no hierarchy
INSERT INTO @resultsDatabaseSchema.@personCodeCountsTable 
    
SELECT DISTINCT
    'Measurements' AS analysis_type,
    pcac.person_id AS person_id,
    pcac.concept_id AS concept_id,
    pcac.n_records AS n_records,
    pcac.first_date AS first_date,
    pcac.first_age AS first_age,
    pcac.aggregated_value AS aggregated_value,
    pcac.aggregated_value_unit AS aggregated_value_unit,
    pcac.aggregated_category AS aggregated_category
FROM @resultsDatabaseSchema.@personCodeAtomicCountsTable AS pcac;


DROP TABLE IF EXISTS @resultsDatabaseSchema.temp_concept_ancestor;