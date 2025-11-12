#
# checkCohortData
#
test_that("checkCohortData works", {
  cohortData <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortData |> assertCohortData()
  cohortData |>
    checkCohortData() |>
    expect_true()
})

test_that("checkCohortData fails with missing column", {
  cohortData <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  ) |>
    select(-cohort_end_date)

  cohortData |>
    checkCohortData() |>
    expect_match("cohort_end_date")
})

test_that("checkCohortData fails with wrong type", {
  cohortData <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  ) |>
    mutate(cohort_end_date = as.character(cohort_end_date))

  cohortData |>
    checkCohortData() |>
    expect_match("cohort_end_date is not of type date")
})

test_that("checkCohortData fails with missing values in cohort_name", {
  cohortData <- tibble::tibble(
    cohort_name = rep(c(as.character(NA), "Cohort B"), 5), # changed name
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortData |>
    checkCohortData() |>
    expect_match("rows are missing cohort_name")
})

test_that("checkCohortData fails with cohort_start_date > cohort_end_date", {
  cohortData <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2022-01-01")), 5), # changed dates second value
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortData |>
    checkCohortData() |>
    expect_match("rows have cohort_start_date older than cohort_end_date")
})



#
# cohortDataToCohortDefinitionSet
#

test_that(" cohortDataToCohortDefinitionSet works", {
  cohortData <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(cohortData)

  cohortDefinitionSet |> checkmate::expect_tibble()
  cohortDefinitionSet |>
    names() |>
    checkmate::expect_names(must.include = c("cohortId", "cohortName", "json", "sql"))
  cohortDefinitionSet |>
    pull(json) |>
    stringr::str_detect('cohortType\": \"FromCohortData\"') |>
    all() |>
    expect_true()
  (cohortDefinitionSet |> pull(sql) |> unique() |> length() == cohortDefinitionSet |> nrow()) |> expect_true()
  cohortDefinitionSet$sql[[1]] |>
    stringr::str_detect("WHERE cd.cohort_definition_id = 1") |>
    expect_true()
  cohortDefinitionSet$sql[[2]] |>
    stringr::str_detect("WHERE cd.cohort_definition_id = 2") |>
    expect_true()
})


test_that(" cohortDataToCohortDefinitionSet works with ", {
  cohortData <- tibble::tibble(
    cohort_name = rep(c("Cohort A", "Cohort B"), 5),
    person_source_value = letters[1:10],
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 5),
    cohort_end_date = rep(as.Date(c("2020-01-03", "2020-01-04")), 5)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(cohortData, newCohortIds = c(33, 44))

  cohortDefinitionSet |> checkmate::expect_tibble()
  cohortDefinitionSet |>
    names() |>
    checkmate::expect_names(must.include = c("cohortId", "cohortName", "json", "sql"))
  cohortDefinitionSet |>
    pull(json) |>
    stringr::str_detect('cohortType\": \"FromCohortData\"') |>
    all() |>
    expect_true()
  (cohortDefinitionSet |> pull(sql) |> unique() |> length() == cohortDefinitionSet |> nrow()) |> expect_true()
  cohortDefinitionSet$sql[[1]] |>
    stringr::str_detect("WHERE cd.cohort_definition_id = 33") |>
    expect_true()
  cohortDefinitionSet$sql[[2]] |>
    stringr::str_detect("WHERE cd.cohort_definition_id = 44") |>
    expect_true()
})




#
# getCohortDataFromCohortTable
#
test_that("getCohortDataFromCohortTable returns a cohort", {
  connection <- helper_createNewConnection()

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- helper_tableNameWithTimestamp("test_cohort")

  withr::defer({
    CohortGenerator_dropCohortStatsTables(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = getCohortTableNames(cohortTableName)
    )
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

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

  # ignore warnings displayed every 8 hours
  suppressWarnings({
    cohortData <- getCohortDataFromCohortTable(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTableName,
      cohortNameIds = generatedCohorts |> dplyr::select(cohortId, cohortName)
    )
  })

  cohortData |>
    checkCohortData() |>
    expect_true()
  cohortData |>
    nrow() |>
    expect_gt(0)
})
