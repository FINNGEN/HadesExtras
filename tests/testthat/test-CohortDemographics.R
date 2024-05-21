
test_that("executeCohortOverlaps works", {

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

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)


  exportFolder <- file.path(tempdir(), "cohortDemographics")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({unlink(exportFolder, recursive = TRUE);gc()})


  executeCohortDemographicsCounts(
    exportFolder = exportFolder,
    cohortTableHandler = cohortTableHandler,
    cohortIds = c(1,2)
  )

  expect_true(file.exists(file.path(exportFolder, "demographics_counts.csv")))

  CohortOverlapsResults <- read.csv(file.path(exportFolder, "demographics_counts.csv"))  |>
    tibble::as_tibble()

  suppressWarnings(
    HadesExtras::csvFilesToSqlite(
      dataFolder = exportFolder,
      sqliteDbPath = file.path(exportFolder, "analysisResults.sqlite"),
      overwrite = TRUE,
      analysis = "cohortDemographics"
    )
  )



})



test_that("getCohortDemographics works", {


  testthat::skip_if(testSelectedConfiguration$connection$connectionDetailsSettings$dbms!="eunomia")

  connection <- helper_createNewConnection()
  #on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

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
    #packageName = "HadesExtras",
    verbose = FALSE
  )

  generatedCohorts <- CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName),
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
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

  demographics  <- getCohortDemographics(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTable = testSelectedConfiguration$cohortTable$cohortTableName,
    cohortIds = c(10, 20)
  )

  demographics |> checkmate::expect_tibble()


})
