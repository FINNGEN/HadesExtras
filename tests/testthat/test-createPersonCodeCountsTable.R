test_that("createPersonCodeCountsTable", {

  CDMdb <- createCDMdbHandlerFromList(test_cohortTableHandlerConfig)
  withr::defer({
    rm(CDMdb)
    gc()
  })

  personCodeCountsTable <- "person_code_counts_test"

  createPersonCodeCountsTable(CDMdb, personCodeCountsTable = personCodeCountsTable)

  personCodeCounts <- CDMdb$connectionHandler$tbl(personCodeCountsTable, CDMdb$resultsDatabaseSchema) 

  nRows  <- personCodeCounts |> dplyr::count() |> dplyr::pull(n) 
  nRows |> expect_gt(0)
  # analysis_type
  personCodeCounts |> dplyr::filter(is.na(analysis_type)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # person_id
  personCodeCounts |> dplyr::filter(is.na(person_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # concept_id
  personCodeCounts |> dplyr::filter(is.na(concept_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # n_records
  personCodeCounts |> dplyr::filter(is.na(n_records)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # first_date
  personCodeCounts |> dplyr::filter(is.na(first_date)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # first_age
  personCodeCounts |> dplyr::filter(is.na(first_age)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # aggregated_value
  personCodeCounts |> dplyr::filter(analysis_type !=  'Measurements' & analysis_type != 'ATC' & !is.na(aggregated_value)) |> 
  dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # aggregated_value_unit
  personCodeCounts |> dplyr::filter(analysis_type !=  'Measurements' & analysis_type != 'ATC' & !is.na(aggregated_value)) |> 
  dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # aggregated_category
  personCodeCounts |> dplyr::filter(analysis_type !=  'Measurements' & !is.na(aggregated_category)) |> 
  dplyr::count() |> dplyr::pull(n) |> expect_equal(0)

  #personCodeCounts |> dplyr::group_by(analysis_type) |> dplyr::summarize(n=dplyr::n_distinct(concept_id))
})







test_that("createPersonCodeAtomicCountsTable condition", {

  CDMdb <- createCDMdbHandlerFromList(test_cohortTableHandlerConfig)
  withr::defer({
    rm(CDMdb)
    gc()
  })

  personCodeAtomicCountsTable <- "person_code_atomic_counts_test"

  domains <- tibble::tribble(
    ~domain_id, ~table_name, ~concept_id_field, ~start_date_field, ~end_date_field, ~maps_to_concept_id_field,
    "Condition", "condition_occurrence", "condition_concept_id", "condition_start_date", "condition_end_date", "condition_source_concept_id",
  )

  createPersonCodeAtomicCountsTable(CDMdb, domains = domains, personCodeAtomicCountsTable = personCodeAtomicCountsTable)

  atomicCountsTable <- CDMdb$connectionHandler$tbl(personCodeAtomicCountsTable, CDMdb$resultsDatabaseSchema) 
 
  nRows  <- atomicCountsTable |> dplyr::count() |> dplyr::pull(n) 
  nRows |> expect_gt(0)
  # domain_id
  atomicCountsTable |> dplyr::filter(is.na(domain_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  atomicCountsTable |> dplyr::distinct(domain_id) |> dplyr::pull(domain_id) |> expect_equal("Condition")
  # person_id
  atomicCountsTable |> dplyr::filter(is.na(person_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # concept_id
  atomicCountsTable |> dplyr::filter(is.na(concept_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # maps_to_concept_id
  atomicCountsTable |> dplyr::filter(is.na(maps_to_concept_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # n_records
  atomicCountsTable |> dplyr::filter(is.na(n_records)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # first_date
  atomicCountsTable |> dplyr::filter(is.na(first_date)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # first_age
  atomicCountsTable |> dplyr::filter(is.na(first_age)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # aggregated_value
   atomicCountsTable |> dplyr::filter(is.na(aggregated_value)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(nRows)
  # aggregated_value_unit
  atomicCountsTable |> dplyr::filter(aggregated_value_unit == 0) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(nRows)
  # aggregated_category
   atomicCountsTable |> dplyr::filter(aggregated_category == 0) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(nRows)
})


test_that("createPersonCodeAtomicCountsTable Drug", {

  CDMdb <- createCDMdbHandlerFromList(test_cohortTableHandlerConfig)
  withr::defer({
    rm(CDMdb)
    gc()
  })

  personCodeAtomicCountsTable <- "person_code_atomic_counts_test"

  domains <- tibble::tribble(
    ~domain_id, ~table_name, ~concept_id_field, ~start_date_field, ~end_date_field, ~maps_to_concept_id_field,
    "Drug", "drug_exposure", "drug_concept_id", "drug_exposure_start_date", "drug_exposure_end_date", "drug_concept_id",
  )

  createPersonCodeAtomicCountsTable(CDMdb, domains = domains, personCodeAtomicCountsTable = personCodeAtomicCountsTable)

  atomicCountsTable <- CDMdb$connectionHandler$tbl(personCodeAtomicCountsTable, CDMdb$resultsDatabaseSchema) 
 
  nRows  <- atomicCountsTable |> dplyr::count() |> dplyr::pull(n) 
  nRows |> expect_gt(0)
  # domain_id
  atomicCountsTable |> dplyr::filter(is.na(domain_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  atomicCountsTable |> dplyr::distinct(domain_id) |> dplyr::pull(domain_id) |> expect_equal("Drug")
  # person_id
  atomicCountsTable |> dplyr::filter(is.na(person_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # concept_id
  atomicCountsTable |> dplyr::filter(is.na(concept_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # maps_to_concept_id
  atomicCountsTable |> dplyr::filter(is.na(maps_to_concept_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # n_records
  atomicCountsTable |> dplyr::filter(is.na(n_records)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # first_date
  atomicCountsTable |> dplyr::filter(is.na(first_date)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # first_age
  atomicCountsTable |> dplyr::filter(is.na(first_age)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # aggregated_value
  atomicCountsTable |> dplyr::filter(is.na(aggregated_value)) |> dplyr::count() |> dplyr::pull(n) |> expect_lt(nRows)
  # aggregated_value_unit
  atomicCountsTable |> dplyr::filter(aggregated_value_unit == 8512) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(nRows)
  # aggregated_category
   atomicCountsTable |> dplyr::filter(aggregated_category == 0) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(nRows)
})


test_that("createPersonCodeAtomicCountsTable Measurement", {

  CDMdb <- createCDMdbHandlerFromList(test_cohortTableHandlerConfig)
  withr::defer({
    rm(CDMdb)
    gc()
  })

  personCodeAtomicCountsTable <- "person_code_atomic_counts_test"

  domains <- tibble::tribble(
    ~domain_id, ~table_name, ~concept_id_field, ~start_date_field, ~end_date_field, ~maps_to_concept_id_field,
    "Measurement", "measurement", "measurement_concept_id", "measurement_date",  "measurement_date", "measurement_concept_id",
  )

  createPersonCodeAtomicCountsTable(CDMdb, domains = domains, personCodeAtomicCountsTable = personCodeAtomicCountsTable)

  atomicCountsTable <- CDMdb$connectionHandler$tbl(personCodeAtomicCountsTable, CDMdb$resultsDatabaseSchema) 
 
  nRows  <- atomicCountsTable |> dplyr::count() |> dplyr::pull(n) 
  nRows |> expect_gt(0)
  # domain_id
  atomicCountsTable |> dplyr::filter(is.na(domain_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  atomicCountsTable |> dplyr::distinct(domain_id) |> dplyr::pull(domain_id) |> expect_equal("Measurement")
  # person_id
  atomicCountsTable |> dplyr::filter(is.na(person_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # concept_id
  atomicCountsTable |> dplyr::filter(is.na(concept_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # maps_to_concept_id
  atomicCountsTable |> dplyr::filter(is.na(maps_to_concept_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # n_records
  atomicCountsTable |> dplyr::filter(is.na(n_records)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # first_date
  atomicCountsTable |> dplyr::filter(is.na(first_date)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # first_age
  atomicCountsTable |> dplyr::filter(is.na(first_age)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)

})



test_that("createPersonCodeAtomicCountsTable all", {

  CDMdb <- createCDMdbHandlerFromList(test_cohortTableHandlerConfig)
  withr::defer({
    rm(CDMdb)
    gc()
  })

  personCodeAtomicCountsTable <- "person_code_atomic_counts_test"

  createPersonCodeAtomicCountsTable(CDMdb, personCodeAtomicCountsTable = personCodeAtomicCountsTable)

  atomicCountsTable <- CDMdb$connectionHandler$tbl(personCodeAtomicCountsTable, CDMdb$resultsDatabaseSchema) 
 
  nRows  <- atomicCountsTable |> dplyr::count() |> dplyr::pull(n) 
  nRows |> expect_gt(0)
  # domain_id
  atomicCountsTable |> dplyr::filter(is.na(domain_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  atomicCountsTable |> dplyr::distinct(domain_id) |> dplyr::pull(domain_id) |> checkmate::expect_subset(c("Condition", "Drug", "Measurement", "Observation", "Device", "Procedure"))
  # person_id
  atomicCountsTable |> dplyr::filter(is.na(person_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # concept_id
  atomicCountsTable |> dplyr::filter(is.na(concept_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # maps_to_concept_id
  atomicCountsTable |> dplyr::filter(is.na(maps_to_concept_id)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # n_records
  atomicCountsTable |> dplyr::filter(is.na(n_records)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # first_date
  atomicCountsTable |> dplyr::filter(is.na(first_date)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # first_age
  atomicCountsTable |> dplyr::filter(is.na(first_age)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)

})
