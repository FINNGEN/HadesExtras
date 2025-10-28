test_that("getListOfAnalysis works", {
  analysisRef <- getListOfAnalysis()

  analysisRef |> expect_named(c("analysisId", "analysisName", "domainId", "isBinary", "isStandard", "isSourceConcept"))
  analysisRef |>
    nrow() |>
    expect_gt(0)
})


test_that("FeatureExtraction_createTemporalCovariateSettingsFromList works", {
  analysisIds <- c(1, 41, 101, 141)
  temporalStartDays <- c(1, 2, 3)
  temporalEndDays <- c(4, 5, 6)

  result <- FeatureExtraction_createTemporalCovariateSettingsFromList(analysisIds, temporalStartDays, temporalEndDays)

  result |>
    length() |>
    expect_equal(3)

  result[[1]]$DemographicsGender |> expect_true()
  result[[1]]$ConditionOccurrence |> expect_true()
  result[[1]]$temporalStartDays |> expect_equal(temporalStartDays)
  result[[1]]$temporalEndDays |> expect_equal(temporalEndDays)

  result[[2]]$analyses[[1]]$analysisId |> expect_equal(141)

  result[[3]] |>
    attr("fun") |>
    expect_equal("HadesExtras::YearOfBirth")
})


test_that("FeatureExtraction_createTemporalCovariateSettingsFromList works with strandard", {
  analysisIds <- c(1, 101)
  temporalStartDays <- c(1, 2, 3)
  temporalEndDays <- c(4, 5, 6)

  result <- FeatureExtraction_createTemporalCovariateSettingsFromList(analysisIds, temporalStartDays, temporalEndDays)

  result |>
    length() |>
    expect_equal(1)

  result[[1]]$DemographicsGender |> expect_true()
  result[[1]]$ConditionOccurrence |> expect_true()
  result[[1]]$temporalStartDays |> expect_equal(temporalStartDays)
  result[[1]]$temporalEndDays |> expect_equal(temporalEndDays)
})


#
# custom covariate settings
#

test_that("FeatureExtraction_createTemporalCovariateSettingsFromList works with custom YearOfBirth", {
  analysisIds <- c(41)
  temporalStartDays <- c(1, 2, 3)
  temporalEndDays <- c(4, 5, 6)

  result <- FeatureExtraction_createTemporalCovariateSettingsFromList(analysisIds, temporalStartDays, temporalEndDays)

  result |>
    length() |>
    expect_equal(1)


  result[[1]] |>
    attr("fun") |>
    expect_equal("HadesExtras::YearOfBirth")
})

test_that("FeatureExtraction_createTemporalCovariateSettingsFromList works with custom ATCgroups", {
  analysisIds <- c(342)
  temporalStartDays <- c(1, 2, 3)
  temporalEndDays <- c(4, 5, 6)

  result <- FeatureExtraction_createTemporalCovariateSettingsFromList(analysisIds, temporalStartDays, temporalEndDays)

  result |>
    length() |>
    expect_equal(1)


  result[[1]] |>
    attr("fun") |>
    expect_equal("HadesExtras::ATCgroups")
})

test_that("FeatureExtraction_createTemporalCovariateSettingsFromList works with atribute", {
  analysisIds <- c(141)
  temporalStartDays <- c(1, 2, 3)
  temporalEndDays <- c(4, 5, 6)

  result <- FeatureExtraction_createTemporalCovariateSettingsFromList(analysisIds, temporalStartDays, temporalEndDays)

  result |>
    length() |>
    expect_equal(1)

  result[[1]]$analyses[[1]]$analysisId |> expect_equal(141)
})

#
# all are computed
#

test_that("FeatureExtraction_createDetailedTemporalCovariateSettings can run all covariates", {
  skip_if(testingDatabase == "AtlasDevelopment-DBI")
  connection <- helper_createNewConnection()
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- 'test_cohort'

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
    settingsFileName = paste0(basePath, "testdata/asthma/Cohorts.csv"),
    jsonFolder = paste0(basePath, "testdata/asthma/cohorts"),
    sqlFolder = paste0(basePath, "testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = packageName,
    verbose = FALSE
  )

  CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = getCohortTableNames(cohortTableName),
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )

  analysisIds <- getListOfAnalysis() |> dplyr::pull(analysisId)
  covariateSettings <- FeatureExtraction_createTemporalCovariateSettingsFromList(analysisIds)

  covariateData <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTableName,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortIds = c(1, 2),
    aggregated = TRUE, 
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
  )
  covariateData$covariates 
  covariateData$analysisRef |>
    dplyr::collect() |> 
    dplyr::pull(analysisId) |>
    setdiff(analysisIds)  |> 
    expect_length(0)
})

