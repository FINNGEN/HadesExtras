#
# CohortGenerator_createCohortTables
#
test_that("CohortGenerator_createCohortTables creates a cohort table", {
  testthat::skip_if_not(testingDatabase |> stringr::str_starts("AtlasDevelopment"))

  connection <- helper_createNewConnection()
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cohortTableName <- "test_cohort2"

  CohortGenerator_createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName)
  )

  strings <- strsplit(cohortDatabaseSchema, "\\.")
  bq_project <- strings[[1]][1]
  bq_dataset <- strings[[1]][2]

  bq_table <- bigrquery::bq_table(bq_project, bq_dataset, cohortTableName)
  bigrquery::bq_table_exists(bq_table) |> expect_true()

  CohortGenerator_createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName),
    incremental = FALSE
  )

  bq_table <- bigrquery::bq_table(bq_project, bq_dataset, cohortTableName)
  bigrquery::bq_table_exists(bq_table) |> expect_true()
})

#
# CohortGenerator_deleteCohortFromCohortTable
#
test_that("CohortGenerator_deleteCohortFromCohortTable deletes a cohort", {
  connection <- helper_createNewConnection()
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  CohortGenerator_createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName),
  )

  if (interactive()) {
    basePath <- here::here("inst/")
    packageName <- NULL
  } else {
    basePath <- ""
    packageName <- "HadesExtras"
  }

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = paste0(basePath, "testdata/matching/Cohorts.csv"),
    jsonFolder = paste0(basePath, "testdata/matching/cohorts"),
    sqlFolder = paste0(basePath, "testdata/matching/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = packageName,
    verbose = FALSE
  )

  generatedCohorts <- CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName),
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )

  generatedCohorts$cohortId |> expect_equal(c(10, 20))

  resultDelete <- CohortGenerator_deleteCohortFromCohortTable(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName),
    cohortIds = c(10L)
  )

  resultDelete |> expect_true()

  codeCounts <- CohortGenerator::getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName
  )

  codeCounts$cohortId |> expect_equal(20)
})


#
# CohortGenerator_generateCohortSet
#
test_that("cohortDataToCohortDefinitionSet works", {
  # get test settings
  connection <- helper_createNewConnection()
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  CohortGenerator_createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName)
  )

  # test paramssd
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    numberPersons = 2 * 5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  # function
  start_time <- Sys.time()
  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    cohortTableNames = getCohortTableNames(cohortTableName),
    incremental = FALSE
  )
  end_time <- Sys.time()
  print(end_time - start_time)
  # expectations
  cohortGeneratorResults |> checkmate::expect_tibble()
  cohortGeneratorResults |>
    names() |>
    checkmate::expect_names(must.include = c("cohortId", "generationStatus", "startTime", "endTime", "buildInfo"))

  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message |> expect_equal("All person_source_values were found")
})

test_that("cohortDataToCohortDefinitionSet reports missing source person id", {
  # get test settings
  connection <- helper_createNewConnection()
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  CohortGenerator_createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName)
  )

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    numberPersons = 2 * 5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = c(sourcePersonToPersonId$person_source_value[1:5], letters[1:5]),
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  # function
  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    cohortTableNames = getCohortTableNames(cohortTableName),
    incremental = FALSE
  )

  # expectations
  cohortGeneratorResults |> checkmate::expect_tibble()
  cohortGeneratorResults |>
    names() |>
    checkmate::expect_names(must.include = c("cohortId", "generationStatus", "startTime", "endTime", "buildInfo"))

  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message |> expect_equal("2 person_source_values were not found")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message |> expect_equal("3 person_source_values were not found")
})

test_that("cohortDataToCohortDefinitionSet reports missing cohort_start_date", {
  # get test settings
  connection <- helper_createNewConnection()
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  CohortGenerator_createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName)
  )

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    numberPersons = 2 * 5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", NA)), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  # function
  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    cohortTableNames = getCohortTableNames(cohortTableName),
    incremental = FALSE
  )

  # expectations
  cohortGeneratorResults |> checkmate::expect_tibble()
  cohortGeneratorResults |>
    names() |>
    checkmate::expect_names(must.include = c("cohortId", "generationStatus", "startTime", "endTime", "buildInfo"))

  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message[[2]] |> expect_equal("5 cohort_start_dates were missing and set to the first observation date")
})

test_that("cohortDataToCohortDefinitionSet reports missing cohort_end_date", {
  # get test settings
  connection <- helper_createNewConnection()
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  CohortGenerator_createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName)
  )

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    numberPersons = 2 * 5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-04")), 5),
    cohort_end_date = rep(as.Date(c(NA, "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  # function
  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    cohortTableNames = getCohortTableNames(cohortTableName),
    incremental = FALSE
  )

  # expectations
  cohortGeneratorResults |> checkmate::expect_tibble()
  cohortGeneratorResults |>
    names() |>
    checkmate::expect_names(must.include = c("cohortId", "generationStatus", "startTime", "endTime", "buildInfo"))

  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[2]] |> expect_equal("5 cohort_end_dates were missing and set to the first observation date")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")
})


test_that("cohortDataToCohortDefinitionSet also works from imported files", {
  # get test settings
  connection <- helper_createNewConnection()
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  CohortGenerator_createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName)
  )

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    numberPersons = 2 * 5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-04")), 5),
    cohort_end_date = rep(as.Date(c(NA, "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  # save and load
  CohortGenerator::saveCohortDefinitionSet(
    cohortDefinitionSet,
    settingsFileName = file.path(tempdir(), "inst/Cohorts.csv"),
    jsonFolder = file.path(tempdir(), "inst/cohorts"),
    sqlFolder = file.path(tempdir(), "inst/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = file.path(tempdir(), "inst/cohort_subset_definitions/")
  )

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = file.path(tempdir(), "inst/Cohorts.csv"),
    jsonFolder = file.path(tempdir(), "inst/cohorts"),
    sqlFolder = file.path(tempdir(), "inst/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = file.path(tempdir(), "inst/cohort_subset_definitions/")
  )

  # function
  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    cohortTableNames = getCohortTableNames(cohortTableName),
    incremental = FALSE
  )

  # expectations
  cohortGeneratorResults |> checkmate::expect_tibble()
  cohortGeneratorResults |>
    names() |>
    checkmate::expect_names(must.include = c("cohortId", "generationStatus", "startTime", "endTime", "buildInfo"))

  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[2]] |> expect_equal("5 cohort_end_dates were missing and set to the first observation date")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")
})


test_that("cohortDataToCohortDefinitionSet incremental mode do not create the tmp cohortData", {
  # get test settings
  connection <- helper_createNewConnection()
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  CohortGenerator_createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName)
  )

  # test params
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    numberPersons = 2 * 5
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  incrementalFolder <- file.path(tempdir(), digest::digest(Sys.time()))
  withr::defer({
    unlink(incrementalFolder, recursive = TRUE)
  })

  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    cohortTableNames = getCohortTableNames(cohortTableName),
    incremental = TRUE,
    incrementalFolder = incrementalFolder
  )

  # expectations first run
  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "COMPLETE"))
  cohortGeneratorResults$buildInfo[[1]]$logTibble$message |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[2]]$logTibble$message |> expect_equal("All person_source_values were found")


  cohort_data <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = sourcePersonToPersonId$person_source_value,
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c(NA, "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data
  )

  cohortGeneratorResults <- CohortGenerator_generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortDefinitionSet = cohortDefinitionSet,
    cohortTableNames = getCohortTableNames(cohortTableName),
    incremental = TRUE,
    incrementalFolder = incrementalFolder
  )

  # expectations firt run
  cohortGeneratorResults$generationStatus |> expect_equal(c("COMPLETE", "SKIPPED"))

  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[1]] |> expect_equal("All person_source_values were found")
  cohortGeneratorResults$buildInfo[[1]]$logTibble$message[[2]] |> expect_equal("5 cohort_end_dates were missing and set to the first observation date")
  cohortGeneratorResults$buildInfo[[2]]$logTibble |>
    nrow() |>
    expect_equal(0)
})



#
# CohortGenerator_getCohortsOverlaps
#
test_that("CohortGenerator_getCohortsOverlaps works", {
  # get test settings
  connection <- helper_createNewConnection()

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  withr::defer({
    
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  # test params
  cohort_data <- tibble::tibble(
    cohort_definition_id = c(rep(10, 5), rep(20, 5)),
    subject_id = c(1, 2, 3, 4, 5, 1, 2, 3, 9, 10),
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  suppressWarnings({
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = cohortDatabaseSchema,
      tableName = cohortTableName,
      data = cohort_data
    )
  })

  # function
  cohortOverlaps <- CohortGenerator_getCohortsOverlaps(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName
  ) |> dplyr::arrange()

  # expectations
  cohortOverlaps |> checkmate::expect_tibble()
  cohortOverlaps |>
    names() |>
    checkmate::expect_names(must.include = c("cohortIdCombinations", "numberOfSubjects"))
  cohortOverlaps |>
    nrow() |>
    expect_equal(3)

  cohortOverlaps |>
    dplyr::filter(
      stringr::str_detect(cohortIdCombinations, "10") &
        stringr::str_detect(cohortIdCombinations, "20")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(3)
  cohortOverlaps |>
    dplyr::filter(
      !stringr::str_detect(cohortIdCombinations, "10") &
        stringr::str_detect(cohortIdCombinations, "20")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(2)
  cohortOverlaps |>
    dplyr::filter(
      stringr::str_detect(cohortIdCombinations, "10") &
        !stringr::str_detect(cohortIdCombinations, "20")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(2)
})


test_that("CohortGenerator_getCohortsOverlaps works no overlap", {
  # get test settings
  connection <- helper_createNewConnection()

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  withr::defer({
    
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  # test params
  cohort_data <- tibble::tibble(
    cohort_definition_id = c(rep(10, 5), rep(20, 5)),
    subject_id = c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )
  suppressWarnings({
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = cohortDatabaseSchema,
      tableName = cohortTableName,
      data = cohort_data
    )
  })

  # function
  cohortOverlaps <- CohortGenerator_getCohortsOverlaps(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName
  ) |> dplyr::arrange()

  # expectations
  cohortOverlaps |> checkmate::expect_tibble()
  cohortOverlaps |>
    names() |>
    checkmate::expect_names(must.include = c("cohortIdCombinations", "numberOfSubjects"))
  cohortOverlaps |>
    nrow() |>
    expect_equal(2)

  cohortOverlaps |>
    dplyr::filter(
      stringr::str_detect(cohortIdCombinations, "-10-") &
        !stringr::str_detect(cohortIdCombinations, "-20-")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(5)
  cohortOverlaps |>
    dplyr::filter(
      !stringr::str_detect(cohortIdCombinations, "-10-") &
        stringr::str_detect(cohortIdCombinations, "-20-")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(5)
})



test_that("CohortGenerator_getCohortsOverlaps works no duplicates", {
  # get test settings
  connection <- helper_createNewConnection()

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  withr::defer({
    
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  # test params
  cohort_data <- tibble::tibble(
    cohort_definition_id = c(rep(10, 5), rep(20, 5)),
    subject_id = c(11, 2, 3, 3, 3, 1, 2, 3, 9, 10),
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  suppressWarnings({
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = cohortDatabaseSchema,
      tableName = cohortTableName,
      data = cohort_data
    )
  })

  # function
  cohortOverlaps <- CohortGenerator_getCohortsOverlaps(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName
  ) |> dplyr::arrange()

  # expectations
  cohortOverlaps |> checkmate::expect_tibble()
  cohortOverlaps |>
    names() |>
    checkmate::expect_names(must.include = c("cohortIdCombinations", "numberOfSubjects"))
  cohortOverlaps |>
    nrow() |>
    expect_equal(3)

  cohortOverlaps |>
    dplyr::filter(
      stringr::str_detect(cohortIdCombinations, "-10-") &
        stringr::str_detect(cohortIdCombinations, "-20-")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(2)
  cohortOverlaps |>
    dplyr::filter(
      !stringr::str_detect(cohortIdCombinations, "-10-") &
        stringr::str_detect(cohortIdCombinations, "-20-")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(3)
  cohortOverlaps |>
    dplyr::filter(
      stringr::str_detect(cohortIdCombinations, "-10-") &
        !stringr::str_detect(cohortIdCombinations, "-20-")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(1)
})




test_that("CohortGenerator_getCohortsOverlaps works no ordered cohortData", {
  # get test settings
  connection <- helper_createNewConnection()

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  withr::defer({
    
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  # test params
  cohort_data <- tibble::tibble(
    cohort_definition_id = c(rep(10, 5), rep(20, 5), rep(30, 5)),
    subject_id = c(1, 2, 3, 4, 5, 1, 2, 3, 14, 15, 1, 2, 3, 24, 25),
    cohort_start_date = rep(as.Date(c("2020-01-01")), 15),
    cohort_end_date = rep(as.Date(c("2020-01-03")), 15)
  ) |> dplyr::sample_n(15)

  suppressWarnings({
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = cohortDatabaseSchema,
      tableName = cohortTableName,
      data = cohort_data
    )
  })

  # function
  cohortOverlaps <- CohortGenerator_getCohortsOverlaps(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName
  ) |> dplyr::arrange()

  # expectations
  cohortOverlaps |> checkmate::expect_tibble()
  cohortOverlaps |>
    names() |>
    checkmate::expect_names(must.include = c("cohortIdCombinations", "numberOfSubjects"))
  cohortOverlaps |>
    nrow() |>
    expect_equal(4)

  cohortOverlaps |>
    dplyr::filter(
      stringr::str_detect(cohortIdCombinations, "-10-") &
        !stringr::str_detect(cohortIdCombinations, "-20-") &
        !stringr::str_detect(cohortIdCombinations, "-30-")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(2)

  cohortOverlaps |>
    dplyr::filter(
      stringr::str_detect(cohortIdCombinations, "-10-") &
        stringr::str_detect(cohortIdCombinations, "-20-") &
        stringr::str_detect(cohortIdCombinations, "-30-")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(3)

  cohortOverlaps |>
    dplyr::filter(
      !stringr::str_detect(cohortIdCombinations, "-10-") &
        stringr::str_detect(cohortIdCombinations, "-20-") &
        !stringr::str_detect(cohortIdCombinations, "-30-")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(2)

  cohortOverlaps |>
    dplyr::filter(
      !stringr::str_detect(cohortIdCombinations, "-10-") &
        !stringr::str_detect(cohortIdCombinations, "-20-") &
        stringr::str_detect(cohortIdCombinations, "-30-")
    ) |>
    dplyr::pull("numberOfSubjects") |>
    expect_equal(2)
})

#
# removeCohortIdsFromCohortOverlapsTable
#
test_that("removeCohortIdsFromCohortOverlapsTable works", {
  cohortOverlaps <- data.frame(
    cohortIdCombinations = c("-1-2-", "-2-3-", "-3-4-"),
    numberOfSubjects = c(10, 15, 20)
  )
  cohortIds <- c(2, 3)

  cohortOverlaps <- removeCohortIdsFromCohortOverlapsTable(cohortOverlaps, cohortIds) |>
    dplyr::arrange(cohortIdCombinations)

  cohortOverlaps$cohortIdCombinations |> expect_equal(c("-1-", "-4-"))
  cohortOverlaps$numberOfSubjects |> expect_equal(c(10, 20))

  cohortOverlaps <- removeCohortIdsFromCohortOverlapsTable(cohortOverlaps, c()) |>
    dplyr::arrange(cohortIdCombinations)

  cohortOverlaps$cohortIdCombinations |> expect_equal(c("-1-", "-4-"))
  cohortOverlaps$numberOfSubjects |> expect_equal(c(10, 20))
})


#
# CohortGenerator_dropCohortStatsTables
#
test_that("CohortGenerator_dropCohortStatsTables works", {
  testthat::skip_if_not(testingDatabase |> stringr::str_starts("AtlasDevelopment"))

  connection <- helper_createNewConnection()

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cohortTableName <- "test_cohort2"

  withr::defer({
    
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  CohortGenerator_createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName)
  )

  strings <- strsplit(cohortDatabaseSchema, "\\.")
  bq_project <- strings[[1]][1]
  bq_dataset <- strings[[1]][2]

  bq_table <- bigrquery::bq_table(bq_project, bq_dataset, cohortTableName)
  bigrquery::bq_table_exists(bq_table) |> expect_true()

  CohortGenerator_dropCohortStatsTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName)
  )

  bq_table <- bigrquery::bq_table(bq_project, bq_dataset, cohortTableName)
  bigrquery::bq_table_exists(bq_table) |> expect_false()
})


#
# CohortGenerator_getCohortDemograpics
#
test_that("CohortGenerator_getCohortDemograpics works", {
  # get test settings
  connection <- helper_createNewConnection()

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  vocabularyDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- "test_cohort"

  withr::defer({
    
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  # Create cohort table
  CohortGenerator_createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(cohortTableName)
  )

  # Insert test cohort data
  cohort_data <- tibble::tibble(
    cohort_definition_id = c(rep(10, 3), rep(20, 3)),
    subject_id = c(1, 2, 3, 1, 2, 4),
    cohort_start_date = rep(as.Date(c("2020-01-01", "2021-01-01")), 3),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2021-01-04")), 3)
  )

  suppressWarnings({
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = cohortDatabaseSchema,
      tableName = cohortTableName,
      data = cohort_data
    )
  })

  # Test the function
  cohortDemographics <- CohortGenerator_getCohortDemograpics(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName,
    cohortIds = c(10, 20)
  )

  # Expectations
  
  cohortDemographics |>
    names() |>
    checkmate::expect_names(must.include = c("cohortId", "cohortEntries", "cohortSubjects", "histogramCohortStartYear", "histogramCohortEndYear", "histogramBirthYear", "histogramBirthYearAllEvents", "sexCounts", "sexCountsAllEvents"))
  cohortDemographics |>
    nrow() |>
    expect_equal(2)
  cohortDemographics |>
    dplyr::filter(cohortId == 10) |>
    dplyr::pull("cohortEntries") |>
    expect_equal(3)
  cohortDemographics |>
    dplyr::filter(cohortId == 10) |>
    dplyr::pull("cohortSubjects") |>
    expect_equal(3)
  cohortDemographics |>
    purrr::pluck("histogramCohortStartYear", 1) |>
    expect_equal(tibble::tibble(year = c('2020', '2021'), n = c(2, 1)))
  cohortDemographics |>
    purrr::pluck("histogramCohortEndYear", 1) |>
    expect_equal(tibble::tibble(year = c('2020', '2021'), n = c(2, 1)))
  cohortDemographics |>
    purrr::pluck("sexCounts", 1) |>
    expect_equal(tibble::tibble(sex = c('FEMALE', 'MALE'), n = c(1, 2)))
  cohortDemographics |>
    purrr::pluck("sexCountsAllEvents", 1) |>
    expect_equal(tibble::tibble(sex = c('FEMALE', 'MALE'), n = c(1, 2)))
})


