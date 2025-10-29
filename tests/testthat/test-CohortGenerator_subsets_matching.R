#
# createMatchingSubset
#
test_that("Matching subset naming and instantitation", {
  matchingSubsetNamed <- createMatchingSubset(
    name = NULL,
    matchToCohortId = 11,
    matchRatio = 10,
    matchSex = TRUE,
    matchBirthYear = TRUE,
    matchCohortStartDateWithInDuration = FALSE,
    newCohortStartDate = "asMatch",
    newCohortEndDate = "asMatch"
  )
  expectedName <- "Match to cohort 11 by sex and birth year with ratio 1:10; cohort start date as in matched subject; cohort end date as in matched subject"
  expect_equal(expectedName, matchingSubsetNamed$name)

  matchingSubsetNamed$name <- "foo"
  expect_equal("foo", matchingSubsetNamed$name)

  matchingSubsetNamed <- createMatchingSubset(
    matchToCohortId = 110,
    matchSex = TRUE,
    matchBirthYear = FALSE,
    matchRatio = 100,
    newCohortStartDate = "keep",
    newCohortEndDate = "keep"
  )
  expectedName <- "Match to cohort 110 by sex with ratio 1:100"
  expect_equal(expectedName, matchingSubsetNamed$name)
})



test_that("Matching Subset works", {
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

  # cohort 10:
  # 1 M born in 1970
  # 1 F born in 1971
  #
  # cohort 20:
  # 10 M born in 1970
  # 10 F born in 1970
  # 10 F born in 1971
  # 10 F born in 1972

  # Match to sex only, match ratio 20
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 300,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 10,
        matchRatio = 20,
        matchSex = TRUE,
        matchBirthYear = TRUE,
        matchCohortStartDateWithInDuration = TRUE,
        newCohortStartDate = "keep",
        newCohortEndDate = "keep"
      )
    )
  )

  cohortDefinitionSetWithSubsetDef <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 20)

  generatedCohorts <- CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(cohortTableName),
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
})



test_that("Matching Subset works for different parameters", {
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

  # cohort 10:
  # 1 M born in 1970
  # 1 F born in 1971
  #
  # cohort 20:
  # 10 M born in 1970
  # 10 F born in 1970
  # 10 F born in 1971
  # 10 F born in 1972

  # Match to sex only, match ratio 20
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 300,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 10,
        matchRatio = 20,
        matchSex = TRUE,
        matchBirthYear = FALSE,
        matchCohortStartDateWithInDuration = FALSE,
        newCohortStartDate = "keep",
        newCohortEndDate = "keep"
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

  cohortDemographics$sexCounts[[3]]$n[[1]] |> expect_equal(20) # female
  cohortDemographics$sexCounts[[3]]$n[[2]] |> expect_equal(10) # male, there is only 10 in controls


  # Match to sex and bday, match ratio 20
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 300,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 10,
        matchRatio = 20,
        matchSex = TRUE,
        matchBirthYear = TRUE,
        matchCohortStartDateWithInDuration = FALSE,
        newCohortStartDate = "keep",
        newCohortEndDate = "keep"
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

  cohortDemographics$sexCounts[[3]]$n[[1]] |> expect_equal(10) # female, there is only 10
  cohortDemographics$sexCounts[[3]]$n[[2]] |> expect_equal(10) # male, there is only 10 in controls
  cohortDemographics$histogramBirthYear[[3]]$n[[1]] |> expect_equal(10) # 1970, there is only 10 in controls
  cohortDemographics$histogramBirthYear[[3]]$n[[2]] |> expect_equal(10) # 1971, there is only 10 in controls


  # Match to sex and bday and start day with in observation, keep startday
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 300,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 10,
        matchRatio = 20,
        matchSex = TRUE,
        matchBirthYear = TRUE,
        matchCohortStartDateWithInDuration = TRUE,
        newCohortStartDate = "asMatch",
        newCohortEndDate = "keep"
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

  cohortDemographics$sexCounts[[3]]$n[[1]] |> expect_equal(10) # female, half
  cohortDemographics$sexCounts[[3]]$n[[2]] |> expect_equal(6) # male, half
  cohortDemographics$histogramBirthYear[[3]]$n[[1]] |> expect_equal(6) # 1970, there is only 10 in controls
  cohortDemographics$histogramBirthYear[[3]]$n[[2]] |> expect_equal(10) # 1971, there is only 10 in controls

  cohortDemographics$histogramBirthYear[[1]]$year |> expect_equal(cohortDemographics$histogramBirthYear[[3]]$year)
})
