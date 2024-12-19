test_that("covariateData_YearOfBirth works", {
  connection <- helper_createNewConnection()
  withr::defer({
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- 'test_cohort'

  # set data
  testTable <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1, 1, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 2, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 3, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 4, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2004-01-01"), as.Date("2004-12-01"),
    2, 2, as.Date("2001-01-01"), as.Date("2002-12-01"), # non overplaping
    2, 3, as.Date("2000-06-01"), as.Date("2000-09-01"), # inside
    2, 4, as.Date("2000-06-01"), as.Date("2010-12-01"), # overlap
    2, 5, as.Date("2004-06-01"), as.Date("2010-12-01"), # overlap with second
    2, 6, as.Date("2000-01-01"), as.Date("2010-12-01")
  ) |> 
  dplyr::mutate(
    cohort_definition_id = as.integer(cohort_definition_id),
    subject_id = as.integer(subject_id)
  )

  suppressWarnings({
    DatabaseConnector::insertTable(
      connection = connection,
      table = cohortTableName,
      data = testTable
    )
  })

  covariateSettings <- covariateData_YearOfBirth()

  covariate_control <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTableName,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
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

  # aggregated data
  covariateSettings <- covariateData_YearOfBirth()

  covariate_control <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTableName,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
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
