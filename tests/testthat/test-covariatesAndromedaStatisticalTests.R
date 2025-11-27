test_that("addStatisticalTestsToCovariatesAndromeda parallel returns correct value", {
  covariatesAndromeda <- Andromeda::loadAndromeda("covariatesAndromeda.zip")
  covariatesAndromeda <- Andromeda::copyAndromeda(covariatesAndromeda)

  comparisonsTible <- tibble::tibble(
    comparisonId = 1,
    caseCohortId = 1,
    controlCohortId = 2
  )

#  t0 <- Sys.time()
  covariatesAndromeda <- addStatisticalTestsToCovariatesAndromeda(
    covariatesAndromeda = covariatesAndromeda,
    comparisonsTible = comparisonsTible,
    analysisTypes = c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous"),
    nChunks = NULL
  )
#  td <- Sys.time() - t0

  covariatesAndromeda |> Andromeda::saveAndromeda("covariatesTestsAndromeda.zip", maintainConnection = TRUE )

  covariatesAndromeda |>
    names() |>
    expect_setequal(
      c("analysisRef", "comparisons", "cohortCounts", "conceptRef", "covariates", "covariatesContinuous", "statisticalTests")
    )

  statisticalTests <- covariatesAndromeda$statisticalTests 
  statisticalTests |> names() |> expect_setequal(c("comparisonId", "analysisId", "unit", "caseCohortId", "controlCohortId", "conceptId", "pValue", "effectSize", "standarizeMeanDifference", "testName"))
  nRows <- statisticalTests |> dplyr::count() |> dplyr::pull(n)
  # analysisId
  statisticalTests |> dplyr::filter(is.na(analysisId)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # caseCohortId
  statisticalTests |> dplyr::filter(is.na(caseCohortId)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # controlCohortId
  statisticalTests |> dplyr::filter(is.na(controlCohortId)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # conceptId
  statisticalTests |> dplyr::filter(is.na(conceptId)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # pValue
  statisticalTests |>  dplyr::filter(!stringr::str_detect(testName, "Error"))  |> dplyr::filter(is.na(pValue)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # effectSize
  statisticalTests |>  dplyr::filter(!stringr::str_detect(testName, "Error")) |> dplyr::filter(is.na(effectSize)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # standarizeMeanDifference
  statisticalTests |>  dplyr::filter(!stringr::str_detect(testName, "Error")) |> dplyr::filter(is.na(standarizeMeanDifference)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)
  # testName
  statisticalTests |> dplyr::filter(is.na(testName)) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(0)

  #
  statisticalTests |> dplyr::distinct(comparisonId, analysisId, unit, caseCohortId, controlCohortId, conceptId) |> dplyr::count() |> dplyr::pull(n) |> expect_equal(nRows)
})
