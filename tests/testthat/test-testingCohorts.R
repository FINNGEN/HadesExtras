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
    settingsFileName = here::here("inst/testdata/fracture/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/fracture/cohorts"),
    sqlFolder = here::here("inst/testdata/fracture/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    # packageName = "HadesExtras",
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
    settingsFileName = here::here("inst/testdata/asthma/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/asthma/cohorts"),
    sqlFolder = here::here("inst/testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    # packageName = "HadesExtras",
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
    settingsFileName = here::here("inst/testdata/matching/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/matching/cohorts"),
    sqlFolder = here::here("inst/testdata/matching/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    # packageName = "HadesExtras",
    verbose = FALSE
  )
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()  |> 
    dplyr::arrange(cohortId)

  cohortCounts |> checkmate::expect_tibble(nrows = 2)
  cohortCounts$cohortName |> expect_equal(c("Matching cases", "Matching controls"))
})
