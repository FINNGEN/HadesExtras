
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


  exportFolder <- file.path(tempdir(), "CodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({unlink(exportFolder, recursive = TRUE);gc()})


  executeCohortOverlaps(
    exportFolder = exportFolder,
    cohortTableHandler = cohortTableHandler,
    cohortIds = c(1,2),
    minCellCount = 1
  )

  expect_true(file.exists(file.path(exportFolder, "CohortOverlaps_results.csv")))

  CohortOverlapsResults <- read.csv(file.path(exportFolder, "CohortOverlaps_results.csv"))  |> tibble::as_tibble()

})
