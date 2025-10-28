#
# createMatchingSubset
#
test_that("Operation subset naming and instantitation", {
  operationSubsetNamed <- createOperationSubset(
    name = NULL,
    operationString = "1Upd2"
  )
  expectedName <- "Operation: 1Upd2"
  expect_equal(expectedName, operationSubsetNamed$name)

  operationSubsetNamed$name <- "foo"
  expect_equal("foo", operationSubsetNamed$name)
})



test_that("Operation Subset works", {
  testthat::skip_if_not(testingDatabase |> stringr::str_starts("Eunomia"))

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- helper_tableNameWithTimestamp("test_cohort") 
  
  connection <- helper_createNewConnection()
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
    cohortTableNames = getCohortTableNames(cohortTableName)
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


  # Match to sex only, match ratio 20
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 300,
    subsetOperators = list(
      createOperationSubset(
        name = NULL,
        operationString = "10Upd20"
      )
    )
  )

  cohortDefinitionSetWithSubsetDef <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 20)

  generatedCohorts <- CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName),
    cohortDefinitionSet = cohortDefinitionSetWithSubsetDef,
    incremental = FALSE
  )

  cohortDemographics <- CohortGenerator_getCohortDemograpics(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName
  )

  checkmate::expect_tibble(cohortDemographics)
  cohortDemographics |>
    dplyr::pull(cohortId) |>
    expect_equal(c(10, 20, 20300))
  cohortDemographics |>
    dplyr::pull(cohortEntries) |>
    expect_equal(c(2, 40, 42))
  cohortDemographics |>
    dplyr::pull(cohortSubjects) |>
    expect_equal(c(2, 40, 42))
})
