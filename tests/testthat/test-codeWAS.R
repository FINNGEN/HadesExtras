
test_that("executeCodeWAS works", {

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

  analysisIds  <- c(101, 141, 1, 2, 402, 702, 41)

  exportFolder <- file.path(tempdir(), "CodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({unlink(exportFolder, recursive = TRUE);gc()})

  executeCodeWAS(
    exportFolder = exportFolder,
    cohortTableHandler = cohortTableHandler,
    cohortIdCases = 1,
    cohortIdControls = 2,
    analysisIds = analysisIds,
    covariatesIds = c(8507001, 1041),
    minCellCount = 1
  )

  expect_true(file.exists(file.path(exportFolder, "codewas_results.csv")))

  codeWASResults <- read.csv(file.path(exportFolder, "codewas_results.csv"))  |> tibble::as_tibble()
  codeWASResults |> dplyr::filter(covariate_id == 1002)  |> nrow() |> expect_equal(1)
  codeWASResults |> dplyr::filter(covariate_id == 1041)  |> nrow() |> expect_equal(0)
  codeWASResults |> dplyr::filter(covariate_id == 8507001)  |> nrow() |> expect_equal(0)
  codeWASResults |> dplyr::filter( stringr::str_detect(covariate_id, "101$"))  |> nrow() |> expect_gt(0)

})



