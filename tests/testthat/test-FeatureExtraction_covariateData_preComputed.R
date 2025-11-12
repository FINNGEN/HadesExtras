test_that("preComputed returns correct value", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  personCodeCountsTable <- "person_code_counts_test"

  createPersonCodeCountsTable(cohortTableHandler, personCodeCountsTable = personCodeCountsTable)

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

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  preComputedAnalysis <- getListOfPreComputedAnalysis(cohortTableHandler, personCodeCountsTable = personCodeCountsTable)

  results <- getPreComputedCovariates(
    connection = cohortTableHandler$connectionHandler$getConnection(),
    cdmDatabaseSchema = cohortTableHandler$cdmDatabaseSchema,
    resultsDatabaseSchema = cohortTableHandler$resultsDatabaseSchema,
    personCodeCountsTable = personCodeCountsTable,
    covariateGroups = preComputedAnalysis,
    covariateTypes = c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous"),
    cohortTable = cohortTableHandler$cohortTableNames$cohortTable,
    cohortIds = c(1, 2),
    aggregated = TRUE,
    minCharacterizationMean = 0
  )


  results$analysisRef |> head()
  results$conceptRef |> head()
  results$covariates |> head()
  results$covariatesContinuous |> head()



  # covariateSettings <- covariateData_preComputed(
  #   resultsDatabaseSchema = cohortTableHandler$resultsDatabaseSchema,
  #   personCodeCountsTable = personCodeCountsTable,
  #   covariateGroups = preComputedAnalysis,
  #   covariateTypes = c("Binary")
  # )

  # covariate_control <- FeatureExtraction::getDbCovariateData(
  #   connection = connection,
  #   cohortTable = cohortTableName,
  #   cohortDatabaseSchema = cohortDatabaseSchema,
  #   cdmDatabaseSchema = cdmDatabaseSchema,
  #   covariateSettings = covariateSettings,
  #   cohortIds = 1,
  #   aggregated = TRUE
  # )

})
