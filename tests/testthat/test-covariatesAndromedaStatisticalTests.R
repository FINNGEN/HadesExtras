test_that("addStatisticalTestsToCovariatesAndromeda returns correct value", {
  
  caseControlTible <- tibble::tibble(
    caseCohortId = 1,
    controlCohortId = 2
  )

  covariatesAndromeda <- addStatisticalTestsToCovariatesAndromeda(
    covariatesAndromeda = covariatesAndromeda,
    caseControlTible = caseControlTible,
    analysisTypes = c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous")
  )

})