test_that("plotStatisticalTests returns a ggplot object", {
  covariatesTestsAndromeda <- Andromeda::loadAndromeda("covariatesTestsAndromeda.zip")
  covariatesTestsAndromeda <- Andromeda::copyAndromeda(covariatesTestsAndromeda)

  conceptIds <- c(201826, 37154674, 947841, 21604180, 3035995, 3019900)

  covariates_reactable <- plotCovariatesTestsAndromeda(covariatesTestsAndromeda, conceptIds = conceptIds)

  expect_true(is.reactable(covariates_reactable))
})

