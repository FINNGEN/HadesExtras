#
# Test if the cohortDefinitionSets shaved in the package are working with the CohortTableHandler
#

test_that("Cohort fracture", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = helper_getTestDataPath("testdata/fracture/Cohorts.csv"),
    jsonFolder = helper_getTestDataPath("testdata/fracture/cohorts"),
    sqlFolder = helper_getTestDataPath("testdata/fracture/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = NULL,
    verbose = FALSE
)
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()  |> 
    dplyr::arrange(cohortName)

  cohortCounts |> checkmate::expect_tibble(nrows = 2)
  cohortCounts$cohortName |> expect_equal(c("fracture [HadesExtras]", "fracture-controls [HadesExtras]"))
})


test_that("Cohort asthma", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = helper_getTestDataPath("testdata/asthma/Cohorts.csv"),
    jsonFolder = helper_getTestDataPath("testdata/asthma/cohorts"),
    sqlFolder = helper_getTestDataPath("testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = NULL,
    verbose = FALSE
)
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()  |> 
    dplyr::arrange(cohortId)

  cohortCounts |> checkmate::expect_tibble(nrows = 2)
  cohortCounts$cohortName |> expect_equal(c("asthma [HadesExtras]", "asthma controls [HadesExtras]"))
})


test_that("Cohort matching", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = helper_getTestDataPath("testdata/matching/Cohorts.csv"),
    jsonFolder = helper_getTestDataPath("testdata/matching/cohorts"),
    sqlFolder = helper_getTestDataPath("testdata/matching/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = NULL,
    verbose = FALSE
  )
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()  |> 
    dplyr::arrange(cohortId)

  cohortCounts |> checkmate::expect_tibble(nrows = 2)
  cohortCounts$cohortName |> expect_equal(c("Matching cases", "Matching controls"))
})
