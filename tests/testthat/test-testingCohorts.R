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
    settingsFileName = ("testdata/fracture/Cohorts.csv"),
    jsonFolder = ("testdata/fracture/cohorts"),
    sqlFolder = ("testdata/fracture/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = "HadesExtras",
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
    settingsFileName = ("testdata/asthma/Cohorts.csv"),
    jsonFolder = ("testdata/asthma/cohorts"),
    sqlFolder = ("testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = "HadesExtras",
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
    settingsFileName = ("testdata/matching/Cohorts.csv"),
    jsonFolder = ("testdata/matching/cohorts"),
    sqlFolder = ("testdata/matching/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = "HadesExtras",
    verbose = FALSE
  )
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()  |> 
    dplyr::arrange(cohortId)

  cohortCounts |> checkmate::expect_tibble(nrows = 2)
  cohortCounts$cohortName |> expect_equal(c("Matching cases", "Matching controls"))
})
