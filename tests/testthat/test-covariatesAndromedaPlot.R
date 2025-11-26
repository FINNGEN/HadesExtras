test_that("plotStatisticalTests returns a ggplot object", {
  covariatesTestsAndromeda <- Andromeda::loadAndromeda("covariatesTestsAndromeda.zip")

  covariates_reactable <- plotCovariatesTestsAndromeda(covariatesTestsAndromeda)

  expect_true(is.reactable(covariates_reactable))
})

