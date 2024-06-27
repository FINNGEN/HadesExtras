
test_that("executeTimeCodeWAS works", {

  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  # cohorts from eunomia
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/asthma/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/asthma/cohorts"),
    sqlFolder = here::here("inst/testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/asthma/cohort_subset_definitions/"),
    #packageName = "HadesExtras",
    verbose = FALSE
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  # Match to sex and bday, match ratio 10
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 1,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 1,
        matchRatio = 10,
        matchSex = TRUE,
        matchBirthYear = TRUE,
        matchCohortStartDateWithInDuration = FALSE,
        newCohortStartDate = "asMatch",
        newCohortEndDate = "keep"
      )
    )
  )

  cohortDefinitionSetWithSubsetDef <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 2)

  suppressWarnings(
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSetWithSubsetDef)
  )

  analysisIds = c(101, 102, 141, 204, 601, 641, 301, 341, 404, 906, 701, 741, 801, 841, 501, 541)
  temporalStartDays = c(   -365*2, -365*1, 0,     1,   365+1 )
  temporalEndDays =   c( -365*1-1,     -1, 0, 365*1,   365*2)


  exportFolder <- file.path(tempdir(), "timeCodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({unlink(exportFolder, recursive = TRUE);gc()})

  suppressWarnings(
  executeTimeCodeWAS(
    exportFolder = exportFolder,
    cohortTableHandler = cohortTableHandler,
    cohortIdCases = 1,
    cohortIdControls = 2001,
    analysisIds = analysisIds,
    temporalStartDays = temporalStartDays,
    temporalEndDays = temporalEndDays,
    minCellCount = 1
  )
  )

  expect_true(file.exists(file.path(exportFolder, "temporal_covariate_timecodewas.csv")))

  suppressWarnings(
    HadesExtras::csvFilesToSqlite(
      dataFolder = exportFolder,
      sqliteDbPath = file.path(exportFolder, "analysisResults.sqlite"),
      overwrite = TRUE,
      analysis = "timeCodeWAS"
    )
  )

  analysisResultsHandler  <- ResultModelManager::ConnectionHandler$new(
    connectionDetails = DatabaseConnector::createConnectionDetails(
      dbms = "sqlite",
      server = file.path(exportFolder, "analysisResults.sqlite")
    )
  )
  timeCodeWASResults  <- analysisResultsHandler$tbl("temporal_covariate_timecodewas") |> dplyr::collect()
  # last 3 digits of covariate_it are 101
  timeCodeWASResults |> dplyr::filter(covariate_id %% 1000 == 101)   |> nrow() |> expect_gt(0)
})




test_that("executeTimeCodeWAS works for cohort start date outside of observation paeriod", {

  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  # cohorts from eunomia
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/asthma/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/asthma/cohorts"),
    sqlFolder = here::here("inst/testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/asthma/cohort_subset_definitions/"),
    #packageName = "HadesExtras",
    verbose = FALSE
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  # Match to sex and bday, match ratio 10
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 1,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 1,
        matchRatio = 10,
        matchSex = TRUE,
        matchBirthYear = TRUE,
        matchCohortStartDateWithInDuration = FALSE,
        newCohortStartDate = "asMatch",
        newCohortEndDate = "keep"
      )
    )
  )

  cohortDefinitionSetWithSubsetDef <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 2)

  suppressWarnings(
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSetWithSubsetDef)
  )

  # change one person
  DatabaseConnector::executeSql(
    connection= cohortTableHandler$connectionHandler$getConnection(),
    sql = "UPDATE main.observation_period SET observation_period_start_date = '1984-07-26' WHERE person_id = 9" )

  analysisIds = c(101, 102, 141, 204, 601, 641, 301, 341, 404, 906, 701, 741, 801, 841, 501, 541)
  temporalStartDays = c(   -365*2, -365*1, 0,     1,   365+1 )
  temporalEndDays =   c( -365*1-1,     -1, 0, 365*1,   365*2)


  exportFolder <- file.path(tempdir(), "timeCodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({unlink(exportFolder, recursive = TRUE);gc()})

  suppressWarnings(
  executeTimeCodeWAS(
    exportFolder = exportFolder,
    cohortTableHandler = cohortTableHandler,
    cohortIdCases = 1,
    cohortIdControls = 2001,
    analysisIds = analysisIds,
    temporalStartDays = temporalStartDays,
    temporalEndDays = temporalEndDays,
    minCellCount = 1
  )
  )

  expect_true(file.exists(file.path(exportFolder, "temporal_covariate_timecodewas.csv")))

  suppressWarnings(
    HadesExtras::csvFilesToSqlite(
      dataFolder = exportFolder,
      sqliteDbPath = file.path(exportFolder, "analysisResults.sqlite"),
      overwrite = TRUE,
      analysis = "timeCodeWAS"
    )
  )

  analysisResultsHandler  <- ResultModelManager::ConnectionHandler$new(
    connectionDetails = DatabaseConnector::createConnectionDetails(
      dbms = "sqlite",
      server = file.path(exportFolder, "analysisResults.sqlite")
    )
  )

  expect_true(file.exists(file.path(exportFolder, "temporal_covariate_timecodewas.csv")))
})




test_that("executeTimeCodeWAS works with big size", {

  if(testSelectedConfiguration$database$databaseName != "bigquery500k"){
    skip("Skip test, it is only for bigquery500k")
  }

  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  # cohorts from eunomia
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/fracture/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/fracture/cohorts"),
    sqlFolder = here::here("inst/testdata/fracture/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/fracture/cohort_subset_definitions/"),
    #packageName = "HadesExtras",
    verbose = FALSE
  )

  suppressWarnings(
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  )

  # Match to sex and bday, match ratio 10
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 1,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 1,
        matchRatio = 10,
        matchSex = TRUE,
        matchBirthYear = TRUE,
        matchCohortStartDateWithInDuration = FALSE,
        newCohortStartDate = "asMatch",
        newCohortEndDate = "keep"
      )
    )
  )

  cohortDefinitionSetWithSubsetDef <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 2)

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSetWithSubsetDef)


  analysisIds = c(101, 102, 141, 204, 601, 641, 301, 341, 404, 906, 701, 741, 801, 841, 501, 541)
  temporalStartDays = c(   -365*2, -365*1, 0,     1,   365+1 )
  temporalEndDays =   c( -365*1-1,     -1, 0, 365*1,   365*2)


  exportFolder <- file.path(tempdir(), "timeCodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({unlink(exportFolder, recursive = TRUE);gc()})

  suppressWarnings(
    executeTimeCodeWAS(
      exportFolder = exportFolder,
      cohortTableHandler = cohortTableHandler,
      cohortIdCases = 1,
      cohortIdControls = 2001,
      analysisIds = analysisIds,
      temporalStartDays = temporalStartDays,
      temporalEndDays = temporalEndDays,
      minCellCount = 1
    )
  )

  expect_true(file.exists(file.path(exportFolder, "temporal_covariate_timecodewas.csv")))

  suppressWarnings(
    HadesExtras::csvFilesToSqlite(
      dataFolder = exportFolder,
      sqliteDbPath = file.path(exportFolder, "analysisResults.sqlite"),
      overwrite = TRUE,
      analysis = "timeCodeWAS"
    )
  )

  analysisResultsHandler  <- ResultModelManager::ConnectionHandler$new(
    connectionDetails = DatabaseConnector::createConnectionDetails(
      dbms = "sqlite",
      server = file.path(exportFolder, "analysisResults.sqlite")
    )
  )


  expect_true(file.exists(file.path(exportFolder, "temporal_covariate_timecodewas.csv")))
})



