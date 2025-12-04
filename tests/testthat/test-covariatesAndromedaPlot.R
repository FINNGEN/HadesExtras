test_that("plotStatisticalTests returns a ggplot object", {
  covariatesTestsAndromeda <- Andromeda::loadAndromeda("covariatesTestsAndromeda.zip")
  covariatesTestsAndromeda <- Andromeda::copyAndromeda(covariatesTestsAndromeda)

  conceptIds <- c(201826, 37154674, 947841, 21604180, 3035995, 3019900)

  covariates_reactable <- plotCovariatesTestsAndromeda(covariatesTestsAndromeda, conceptIds = conceptIds)

  expect_true(is.reactable(covariates_reactable))
})




covariatesTestsAndromeda$covariates |> 
filter(conceptId == 201826) |> 
left_join(covariatesTestsAndromeda$analysisRef, by = c("analysisId" = "analysisId")) |> 
collect() |> 
View()
