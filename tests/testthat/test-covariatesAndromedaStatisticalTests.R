test_that("addStatisticalTestsToCovariatesAndromeda parallel returns correct value", {
  covariatesAndromeda <- Andromeda::loadAndromeda("covariatesAndromeda.zip")

  caseControlTible <- tibble::tibble(
    caseCohortId = 1,
    controlCohortId = 2
  )

  t0 <- Sys.time()
  covariatesAndromeda <- addStatisticalTestsToCovariatesAndromeda(
    covariatesAndromeda = covariatesAndromeda,
    caseControlTible = caseControlTible,
    analysisTypes = c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous"),
    nChunks = NULL
  )
  td <- Sys.time() - t0

  covariatesAndromeda |>
    names() |>
    expect_setequal(
      c("analysisRef", "caseControl", "cohortCounts", "conceptRef", "covariates", "covariatesContinuous", "statisticalTests")
    )

  statisticalTests <- covariatesAndromeda$statisticalTests 
  statisticalTests |> names() |> expect_setequal(c("analysisId", "caseCohortId", "controlCohortId", "conceptId", "pValue", "effectSize", "standarizeMeanDifference", "testName"))
  
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
})



