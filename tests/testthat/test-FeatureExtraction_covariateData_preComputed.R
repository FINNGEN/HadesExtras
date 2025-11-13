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

  covariatesAndromeda <- getPreComputedCovariatesAggregated(
    connection = cohortTableHandler$connectionHandler$getConnection(),
    cdmDatabaseSchema = cohortTableHandler$cdmDatabaseSchema,
    cohortTableSchema = cohortTableHandler$cohortDatabaseSchema,
    cohortTable = cohortTableHandler$cohortTableNames$cohortTable,
    cohortIds = c(1, 2),
    resultsDatabaseSchema = cohortTableHandler$resultsDatabaseSchema,
    personCodeCountsTable = personCodeCountsTable,
    covariateGroups = preComputedAnalysis,
    covariateTypes = c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous"),
    minCharacterizationMean = 0
  )


  covariatesAndromeda$analysisRef |> head()
  covariatesAndromeda$conceptRef |> head()
  covariatesAndromeda$covariates |> head()
  covariatesAndromeda$covariatesContinuous |> head()


})
