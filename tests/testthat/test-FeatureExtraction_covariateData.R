test_that("covariateData_YearOfBirth works", {
  connection <- helper_createNewConnection()
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  covariateSettings <- covariateData_YearOfBirth()

  covariate_control <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = "cohort",
    cohortDatabaseSchema = test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema,
    cdmDatabaseSchema = test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortIds = 1,
    aggregated = FALSE
  )

  yearOfBirthValues <- covariate_control$covariates |> dplyr::pull(covariateValue)
  expect_true(all(yearOfBirthValues >= 1900 & yearOfBirthValues < 2000))
  covariate_control$covariates |>
    dplyr::collect() |>
    names() |>
    expect_equal(c("rowId", "covariateId", "covariateValue"))
  covariate_control$covariates |>
    dplyr::collect() |>
    dplyr::distinct(covariateId) |>
    dplyr::pull(covariateId) |>
    expect_equal(1041)
})


test_that("covariateData_YearOfBirth works aggregated", {
  connection <- helper_createNewConnection(addCohorts = TRUE)
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  covariateSettings <- covariateData_YearOfBirth()

  covariate_control <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = "cohort",
    cohortDatabaseSchema = test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema,
    cdmDatabaseSchema = test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortIds = 1,
    aggregated = TRUE
  )

  covariate_control$covariates |>
    dplyr::collect() |>
    nrow() |>
    expect_equal(0)
  covariate_control$covariatesContinuous |>
    dplyr::collect() |>
    nrow() |>
    expect_equal(1)
  covariate_control$covariatesContinuous |>
    dplyr::collect() |>
    names() |>
    expect_equal(c("cohortDefinitionId", "covariateId", "countValue", "minValue", "maxValue", "averageValue", "standardDeviation", "medianValue", "p10Value", "p25Value", "p75Value", "p90Value"))
  covariate_control$covariatesContinuous |>
    dplyr::collect() |>
    dplyr::pull(covariateId) |>
    expect_equal(1041)
})
