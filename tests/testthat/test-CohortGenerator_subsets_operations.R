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

  cohortTableHandlerConfig <- helper_getTestCohortTableHandlerConfig()

  cohortDatabaseSchema <- cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- cohortTableHandlerConfig$cdm$cdmDatabaseSchema
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

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = helper_getTestDataPath("testdata/matching/Cohorts.csv"),
    jsonFolder = helper_getTestDataPath("testdata/matching/cohorts"),
    sqlFolder = helper_getTestDataPath("testdata/matching/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = NULL,
    verbose = FALSE
  )

  # Match to sex only, match ratio 20
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "test",
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


test_that("Operation Subset works with in CohortHandled", {
  testthat::skip_if_not(testingDatabase |> stringr::str_starts("Eunomia"))
  suppressWarnings({
    cohortTableHandler <- helper_createNewCohortTableHandler(
      loadConnectionChecksLevel = "allChecks"
    )
  })
  # withr::defer({
  #   rm(cohortTableHandler)
  #   gc()
  # })

  #tmp
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = helper_getTestDataPath("testdata/asthma/Cohorts.csv"),
    jsonFolder = helper_getTestDataPath("testdata/asthma/cohorts"),
    sqlFolder = helper_getTestDataPath("testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = NULL,
    verbose = FALSE
)

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

   cohortTableHandler$getCohortCounts()
  
  # make operation subset definition
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "test",
    definitionId = 300,
    subsetOperators = list(
      createOperationSubset(
        name = NULL,
        operationString = "1Upd2"
      )
    )
  ) 

  cohortDefinitionSetOp <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 1)

  suppressWarnings({
    cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSetOp)
  })

  cohortTableHandler$getCohortCounts()


  # adding second
  cohortDefinitionSet2 <- cohortDefinitionSet |>
    dplyr::mutate(cohortId = cohortId + 9000) # to avoid conflict with existing cohorts in the handler
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet2)

  a <- cohortTableHandler$getCohortCounts()

  anyNA(a |> dplyr::pull(cohortEntries)) |> expect_false()

})
