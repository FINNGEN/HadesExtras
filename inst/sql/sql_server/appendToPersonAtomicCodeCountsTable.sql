-- Insert into code_stratified_counts table
INSERT INTO @resultsDatabaseSchema.@personCodeAtomicCountsTable

-- calculate counts per each group of concept_id, calendar_year, gender_concept_id, age_decil
SELECT 
        CAST('@domain_id' AS VARCHAR(255)) AS domain_id,
        ccm.person_id AS person_id,
        CAST(ccm.concept_id AS BIGINT) AS concept_id,
        CAST(ccm.maps_to_concept_id AS BIGINT) AS maps_to_concept_id,
        COUNT_BIG(*) AS n_records,
        MIN(ccm.start_date) AS first_date,
        CAST(MIN(ccm.age) AS INTEGER) AS first_age,
        {@domain_id == 'Drug'} ? { CAST(SUM(ccm.value) AS FLOAT) } : {
        {@domain_id == 'Measurement'} ? { CAST(AVG(ccm.value) AS FLOAT) } : 
        { NULL }} AS aggregated_value,
        CAST(MAX(COALESCE(ccm.value_unit, 0)) AS BIGINT) AS aggregated_value_unit,
        CAST(MAX(COALESCE(ccm.category, 0)) AS BIGINT) AS aggregated_category
FROM (
        -- get all person_ids with the concept_id with in a valid observation period
        -- calculate the calendar year, gender_concept_id, age_decile
        -- calculate the min_calendar_year, used to find the first event in history  per code and person 
        SELECT 
                p.person_id AS person_id,
                t.@concept_id_field AS concept_id,
                t.@maps_to_concept_id_field AS maps_to_concept_id,
                t.@start_date_field AS start_date,
                t.@end_date_field AS end_date,
                YEAR(t.@start_date_field) - p.year_of_birth AS age,
                {@domain_id == 'Drug'} ? { NULLIF(DATEDIFF(DAY, t.@start_date_field, t.@end_date_field), 0) } : {
                {@domain_id == 'Measurement'} ? { t.value_as_number } : 
                { NULL }} AS value,
                {@domain_id == 'Drug'} ? { 8512 } : {
                {@domain_id == 'Measurement'} ? { t.unit_concept_id } : 
                { 0 }} AS value_unit,
                {@domain_id == 'Measurement'} ? { t.value_as_concept_id } : 
                { 0 } AS category
        FROM
                @cdmDatabaseSchema.@table_name t
        JOIN 
                @cdmDatabaseSchema.person p
        ON 
                t.person_id = p.person_id
        WHERE
                t.@maps_to_concept_id_field != 0
) ccm
GROUP BY
        ccm.person_id,
        ccm.concept_id,
        ccm.maps_to_concept_id