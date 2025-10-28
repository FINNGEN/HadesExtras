#
# Test if the cohortDefinitionSets shaved in the package are working with the CohortTableHandler
#

test_that("Cohort fracture", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  if (interactive()) {
    basePath <- here::here("inst/")
    packageName <- NULL
  } else {
    basePath <- ""
    packageName <- "HadesExtras"
  }

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = paste0(basePath, "testdata/fracture/Cohorts.csv"),
    jsonFolder = paste0(basePath, "testdata/fracture/cohorts"),
    sqlFolder = paste0(basePath, "testdata/fracture/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = packageName,
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

  if (interactive()) {
    basePath <- here::here("inst/")
    packageName <- NULL
  } else {
    basePath <- ""
    packageName <- "HadesExtras"
  }

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = paste0(basePath, "testdata/asthma/Cohorts.csv"),
    jsonFolder = paste0(basePath, "testdata/asthma/cohorts"),
    sqlFolder = paste0(basePath, "testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = packageName,
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

  if (interactive()) {
    basePath <- here::here("inst/")
    packageName <- NULL
  } else {
    basePath <- ""
    packageName <- "HadesExtras"
  }

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = paste0(basePath, "testdata/matching/Cohorts.csv"),
    jsonFolder = paste0(basePath, "testdata/matching/cohorts"),
    sqlFolder = paste0(basePath, "testdata/matching/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    packageName = packageName,
    verbose = FALSE
  )
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()  |> 
    dplyr::arrange(cohortId)

  cohortCounts |> checkmate::expect_tibble(nrows = 2)
  cohortCounts$cohortName |> expect_equal(c("Matching cases", "Matching controls"))
})
