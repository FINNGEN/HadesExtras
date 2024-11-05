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
  connection <- helper_createNewConnection()
  # on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName)
  )


  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/matching/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/matching/cohorts"),
    sqlFolder = here::here("inst/testdata/matching/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    # packageName = "HadesExtras",
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
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName),
    cohortDefinitionSet = cohortDefinitionSetWithSubsetDef,
    incremental = FALSE
  )

  cohortDemographics <- CohortGenerator_getCohortDemograpics(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTable = testSelectedConfiguration$cohortTable$cohortTableName
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
