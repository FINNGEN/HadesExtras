#
# With covariates
#

test_that("executeCodeWAS works", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({
    rm(cohortTableHandler)
    gc()
  })

  # cohorts from eunomia
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/fracture/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/fracture/cohorts"),
    sqlFolder = here::here("inst/testdata/fracture/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/fracture/cohort_subset_definitions/"),
    # packageName = "HadesExtras",
    verbose = FALSE
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  analysisIds <- c(101, 141, 1, 2, 402, 702, 41)

  exportFolder <- file.path(tempdir(), "CodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({
    unlink(exportFolder, recursive = TRUE)
    gc()
  })

  suppressWarnings(
    executeCodeWAS(
      exportFolder = exportFolder,
      cohortTableHandler = cohortTableHandler,
      cohortIdCases = 1,
      cohortIdControls = 2,
      analysisIds = analysisIds,
      covariatesIds = c(8507001, 1041),
      minCellCount = 1
    )
  )

  expect_true(file.exists(file.path(exportFolder, "codewas_results.csv")))

  codeWASResults <- read.csv(file.path(exportFolder, "codewas_results.csv")) |> tibble::as_tibble()
  codeWASResults |>
    dplyr::filter(covariate_id == 1002) |>
    nrow() |>
    expect_equal(1)
  codeWASResults |>
    dplyr::filter(covariate_id == 1041) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(covariate_id == 8507001) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(stringr::str_detect(covariate_id, "101$")) |>
    nrow() |>
    expect_gt(0)

  suppressWarnings(
    HadesExtras::csvFilesToSqlite(
      dataFolder = exportFolder,
      sqliteDbPath = file.path(exportFolder, "analysisResults.sqlite"),
      overwrite = TRUE,
      analysis = "codeWAS"
    )
  )

  analysisResultsHandler <- ResultModelManager::ConnectionHandler$new(
    connectionDetails = DatabaseConnector::createConnectionDetails(
      dbms = "sqlite",
      server = file.path(exportFolder, "analysisResults.sqlite")
    )
  )
  codeWASResults <- analysisResultsHandler$tbl("codewas_results") |> dplyr::collect()
  codeWASResults |>
    dplyr::filter(covariate_id == 1002) |>
    nrow() |>
    expect_equal(1)
  codeWASResults |>
    dplyr::filter(covariate_id == 1041) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(covariate_id == 8507001) |>
    nrow() |>
    expect_equal(0)
})


test_that("executeCodeWAS warnings with cohort overlap", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({
    rm(cohortTableHandler)
    gc()
  })

  # cohorts from eunomia
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/asthma/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/asthma/cohorts"),
    sqlFolder = here::here("inst/testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/asthma/cohort_subset_definitions/"),
    # packageName = "HadesExtras",
    verbose = FALSE
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  analysisIds <- c(101, 141, 1, 2, 402, 702, 41)

  exportFolder <- file.path(tempdir(), "CodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({
    unlink(exportFolder, recursive = TRUE)
    gc()
  })

  suppressWarnings(
    expect_warning(
      executeCodeWAS(
        exportFolder = exportFolder,
        cohortTableHandler = cohortTableHandler,
        cohortIdCases = 1,
        cohortIdControls = 2,
        analysisIds = analysisIds,
        covariatesIds = c(8507001, 1041),
        minCellCount = 1
      ),
      "patients that are in both cases and controls, cases were removed from controls"
    )
  )

  expect_true(file.exists(file.path(exportFolder, "codewas_results.csv")))

  codeWASResults <- read.csv(file.path(exportFolder, "codewas_results.csv")) |> tibble::as_tibble()
  codeWASResults |>
    dplyr::filter(covariate_id == 1002) |>
    nrow() |>
    expect_equal(1)
  codeWASResults |>
    dplyr::filter(covariate_id == 1041) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(covariate_id == 8507001) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(stringr::str_detect(covariate_id, "101$")) |>
    nrow() |>
    expect_gt(0)
})


test_that("executeCodeWAS works spliting in chuncs", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({
    rm(cohortTableHandler)
    gc()
  })

  # cohorts from eunomia
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/fracture/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/fracture/cohorts"),
    sqlFolder = here::here("inst/testdata/fracture/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/fracture/cohort_subset_definitions/"),
    # packageName = "HadesExtras",
    verbose = FALSE
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  analysisIds <- c(101, 141, 1, 2, 402, 702, 41)

  exportFolder <- file.path(tempdir(), "CodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({
    unlink(exportFolder, recursive = TRUE)
    gc()
  })

  suppressWarnings(
    executeCodeWAS(
      exportFolder = exportFolder,
      cohortTableHandler = cohortTableHandler,
      cohortIdCases = 1,
      cohortIdControls = 2,
      analysisIds = analysisIds,
      covariatesIds = c(8507001, 1041),
      minCellCount = 1,
      chunksSizeNOutcomes = 100
    )
  )

  expect_true(file.exists(file.path(exportFolder, "codewas_results.csv")))

  codeWASResults <- read.csv(file.path(exportFolder, "codewas_results.csv")) |> tibble::as_tibble()
  codeWASResults |>
    dplyr::filter(covariate_id == 1002) |>
    nrow() |>
    expect_equal(1)
  codeWASResults |>
    dplyr::filter(covariate_id == 1041) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(covariate_id == 8507001) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(stringr::str_detect(covariate_id, "101$")) |>
    nrow() |>
    expect_gt(0)
})



test_that("executeCodeWAS works for big size", {
  library(ParallelLogger)
  clearLoggers()
  logger <- createLogger(
    name = "SIMPLE",
    threshold = "INFO",
    appenders = list(createConsoleAppender(layout = layoutTimestamp))
  )

  registerLogger(logger)

  if (testSelectedConfiguration$database$databaseName != "bigquery500k") {
    skip("Skip test, it is only for bigquery500k")
  }

  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({
    rm(cohortTableHandler)
    gc()
  })


  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/fracture/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/fracture/cohorts"),
    sqlFolder = here::here("inst/testdata/fracture/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/fracture/cohort_subset_definitions/"),
    # packageName = "HadesExtras",
    verbose = FALSE
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortTableHandler$getCohortCounts()

  analysisIds <- c(101, 1, 41)

  exportFolder <- file.path(tempdir(), "CodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({
    unlink(exportFolder, recursive = TRUE)
    gc()
  })

  startTime <- Sys.time()
  suppressWarnings(
    executeCodeWAS(
      exportFolder = exportFolder,
      cohortTableHandler = cohortTableHandler,
      cohortIdCases = 1,
      cohortIdControls = 2,
      analysisIds = analysisIds,
      covariatesIds = c(8507001, 1041),
      minCellCount = 1,
      cores = 7
    )
  )
  print(Sys.time() - startTime)


  expect_true(file.exists(file.path(exportFolder, "codewas_results.csv")))

  codeWASResults <- read.csv(file.path(exportFolder, "codewas_results.csv")) |> tibble::as_tibble()
  codeWASResults |>
    dplyr::filter(covariate_id == 1002) |>
    nrow() |>
    expect_equal(1)
  codeWASResults |>
    dplyr::filter(covariate_id == 1041) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(covariate_id == 8507001) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(stringr::str_detect(covariate_id, "101$")) |>
    nrow() |>
    expect_gt(0)
})


#
# Without covariates
#


test_that("executeCodeWAS works with no covariates", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({
    rm(cohortTableHandler)
    gc()
  })

  # cohorts from eunomia
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/fracture/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/fracture/cohorts"),
    sqlFolder = here::here("inst/testdata/fracture/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/fracture/cohort_subset_definitions/"),
    # packageName = "HadesExtras",
    verbose = FALSE
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  analysisIds <- c(101, 141, 1, 2, 402, 702, 41)

  exportFolder <- file.path(tempdir(), "CodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({
    unlink(exportFolder, recursive = TRUE)
    gc()
  })

  suppressWarnings(
    executeCodeWAS(
      exportFolder = exportFolder,
      cohortTableHandler = cohortTableHandler,
      cohortIdCases = 1,
      cohortIdControls = 2,
      analysisIds = analysisIds,
      covariatesIds = NULL,
      minCellCount = 1
    )
  )

  suppressWarnings(
    HadesExtras::csvFilesToSqlite(
      dataFolder = exportFolder,
      sqliteDbPath = file.path(exportFolder, "analysisResults.sqlite"),
      overwrite = TRUE,
      analysis = "codeWAS"
    )
  )

  analysisResultsHandler <- ResultModelManager::ConnectionHandler$new(
    connectionDetails = DatabaseConnector::createConnectionDetails(
      dbms = "sqlite",
      server = file.path(exportFolder, "analysisResults.sqlite")
    )
  )

  codeWASResults <- analysisResultsHandler$tbl("codewas_results") |> dplyr::collect()
  codeWASResults |>
    dplyr::filter(covariate_id == 1002) |>
    nrow() |>
    expect_equal(1)
  codeWASResults |>
    dplyr::filter(covariate_id == 1041) |>
    nrow() |>
    expect_equal(1)
  codeWASResults |>
    dplyr::filter(covariate_id == 8507001) |>
    nrow() |>
    expect_equal(1)

  codeWASResults |>
    dplyr::filter(model_type == "binary") |>
    dplyr::filter(is.na(odds_ratio)) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(model_type == "continuous") |>
    dplyr::filter(is.na(standard_error)) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(is.na(p_value)) |>
    nrow() |>
    expect_equal(0)
})



test_that("executeCodeWAS works when n multiple events per subject", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({
    rm(cohortTableHandler)
    gc()
  })

  # cohorts from eunomia
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/asthma/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/asthma/cohorts"),
    sqlFolder = here::here("inst/testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/asthma/cohort_subset_definitions/"),
    # packageName = "HadesExtras",
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

  analysisIds <- c(101, 141, 1, 2, 402, 702, 41)

  exportFolder <- file.path(tempdir(), "CodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({
    unlink(exportFolder, recursive = TRUE)
    gc()
  })

  suppressWarnings(
    executeCodeWAS(
      exportFolder = exportFolder,
      cohortTableHandler = cohortTableHandler,
      cohortIdCases = 1,
      cohortIdControls = 2001,
      analysisIds = analysisIds,
      covariatesIds = NULL,
      minCellCount = 1
    )
  )

  expect_true(file.exists(file.path(exportFolder, "codewas_results.csv")))

  codeWASResults <- read.csv(file.path(exportFolder, "codewas_results.csv")) |> tibble::as_tibble()
  codeWASResults |>
    dplyr::filter(covariate_id == 1002) |>
    nrow() |>
    expect_equal(1)
  codeWASResults |>
    dplyr::filter(covariate_id == 1041) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(covariate_id == 8507001) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(stringr::str_detect(covariate_id, "101$")) |>
    nrow() |>
    expect_gt(0)

  suppressWarnings(
    HadesExtras::csvFilesToSqlite(
      dataFolder = exportFolder,
      sqliteDbPath = file.path(exportFolder, "analysisResults.sqlite"),
      overwrite = TRUE,
      analysis = "codeWAS"
    )
  )

  analysisResultsHandler <- ResultModelManager::ConnectionHandler$new(
    connectionDetails = DatabaseConnector::createConnectionDetails(
      dbms = "sqlite",
      server = file.path(exportFolder, "analysisResults.sqlite")
    )
  )
  codeWASResults <- analysisResultsHandler$tbl("codewas_results") |> dplyr::collect()
  codeWASResults |>
    dplyr::filter(covariate_id == 1002) |>
    nrow() |>
    expect_equal(1)
  codeWASResults |>
    dplyr::filter(covariate_id == 1041) |>
    nrow() |>
    expect_equal(0)
  codeWASResults |>
    dplyr::filter(covariate_id == 8507001) |>
    nrow() |>
    expect_equal(0)
})
