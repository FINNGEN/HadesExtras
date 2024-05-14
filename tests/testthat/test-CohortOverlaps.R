
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


  exportFolder <- file.path(tempdir(), "cohortOverlaps")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({unlink(exportFolder, recursive = TRUE);gc()})


  executeCohortOverlaps(
    exportFolder = exportFolder,
    cohortTableHandler = cohortTableHandler,
    cohortIds = c(1,2),
    minCellCount = 1
  )

  expect_true(file.exists(file.path(exportFolder, "CohortOverlaps_results.csv")))

  CohortOverlapsResults <- read.csv(file.path(exportFolder, "CohortOverlaps_results.csv"))  |> tibble::as_tibble()  |> dplyr::arrange(cohort_id_combinations)

  CohortOverlapsResults$cohort_id_combinations |> expect_equal(c("-1-", "-2-"))
  CohortOverlapsResults$number_of_subjects |> expect_equal(c(56, 2638))

})



test_that("removeCohortIdsFromCohortOverlapsTable works", {

  cohortOverlaps <- data.frame(cohortIdCombinations = c("-1-2-", "-2-3-", "-3-4-"),
                               numberOfSubjects = c(10, 15, 20))
  cohortIds <- c(2, 3)

  cohortOverlaps <- removeCohortIdsFromCohortOverlapsTable(cohortOverlaps, cohortIds) |>
    dplyr::arrange(cohortIdCombinations)

  cohortOverlaps$cohortIdCombinations |> expect_equal(c("-1-", "-4-"))
  cohortOverlaps$numberOfSubjects |> expect_equal(c(10, 20))

  cohortOverlaps <- removeCohortIdsFromCohortOverlapsTable(cohortOverlaps, c()) |>
    dplyr::arrange(cohortIdCombinations)

  cohortOverlaps$cohortIdCombinations |> expect_equal(c("-1-", "-4-"))
  cohortOverlaps$numberOfSubjects |> expect_equal(c(10, 20))


})
