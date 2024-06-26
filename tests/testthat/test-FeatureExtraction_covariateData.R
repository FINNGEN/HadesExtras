


test_that("covariateData_YearOfBirth works", {

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

  covariateSettings  <- covariateData_YearOfBirth()

  covariate_control <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = testSelectedConfiguration$cohortTable$cohortTableName,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortIds = 10,
    aggregated = F
  )

  covariate_control$covariates  |> dplyr::pull(covariateValue) |> expect_equal(c(1970, 1971))
  covariate_control$covariates  |> dplyr::collect()  |> names()  |>
    expect_equal(c("cohortDefinitionId", "rowId", "covariateId", "covariateValue"))
})


test_that("covariateData_YearOfBirth works aggregated", {

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

  covariateSettings  <- covariateData_YearOfBirth()

  covariate_control <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = testSelectedConfiguration$cohortTable$cohortTableName,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortIds = 10,
    aggregated = T
  )

  covariate_control$covariates  |> dplyr::collect()  |> nrow() |> expect_equal(0)
  covariate_control$covariatesContinuous  |> dplyr::collect()  |> nrow() |> expect_equal(1)
  covariate_control$covariatesContinuous  |> dplyr::collect()  |> names()  |>
    expect_equal(c("cohortDefinitionId", "covariateId", "countValue", "minValue", "maxValue", "averageValue", "standardDeviation", "medianValue", "p10Value", "p25Value", "p75Value", "p90Value"))

})
