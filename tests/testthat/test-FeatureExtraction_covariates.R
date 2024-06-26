test_that("getListOfAnalysis works", {
  analysisRef <- getListOfAnalysis()

  analysisRef |> expect_named(c("analysisId", "analysisName", "domainId", "isBinary", "isStandard", "isSourceConcept"))
  analysisRef |> nrow() |> expect_gt(0)
})


test_that("FeatureExtraction_createTemporalCovariateSettingsFromList works", {
  analysisIds <- c(1, 41, 101, 141)
  temporalStartDays <- c(1, 2, 3)
  temporalEndDays <- c(4, 5, 6)

  result <- FeatureExtraction_createTemporalCovariateSettingsFromList(analysisIds, temporalStartDays, temporalEndDays)

  result  |> length() |> expect_equal(3)

  result[[1]]$DemographicsGender |> expect_true()
  result[[1]]$ConditionOccurrence |> expect_true()
  result[[1]]$temporalStartDays |> expect_equal(temporalStartDays)
  result[[1]]$temporalEndDays |> expect_equal(temporalEndDays)

  result[[2]]$analyses[[1]]$analysisId |> expect_equal(141)

  result[[3]] |> attr("fun") |> expect_equal("HadesExtras::YearOfBirth")
})


test_that("FeatureExtraction_createTemporalCovariateSettingsFromList works with strandard", {
  analysisIds <- c(1,  101)
  temporalStartDays <- c(1, 2, 3)
  temporalEndDays <- c(4, 5, 6)

  result <- FeatureExtraction_createTemporalCovariateSettingsFromList(analysisIds, temporalStartDays, temporalEndDays)

  result  |> length() |> expect_equal(1)

  result[[1]]$DemographicsGender |> expect_true()
  result[[1]]$ConditionOccurrence |> expect_true()
  result[[1]]$temporalStartDays |> expect_equal(temporalStartDays)
  result[[1]]$temporalEndDays |> expect_equal(temporalEndDays)

})

test_that("FeatureExtraction_createTemporalCovariateSettingsFromList works with custom", {
  analysisIds <- c( 41)
  temporalStartDays <- c(1, 2, 3)
  temporalEndDays <- c(4, 5, 6)

  result <- FeatureExtraction_createTemporalCovariateSettingsFromList(analysisIds, temporalStartDays, temporalEndDays)

  result  |> length() |> expect_equal(1)


  result[[1]] |> attr("fun") |> expect_equal("HadesExtras::YearOfBirth")
})

test_that("FeatureExtraction_createTemporalCovariateSettingsFromList works with atribute", {
  analysisIds <- c(141)
  temporalStartDays <- c(1, 2, 3)
  temporalEndDays <- c(4, 5, 6)

  result <- FeatureExtraction_createTemporalCovariateSettingsFromList(analysisIds, temporalStartDays, temporalEndDays)

  result  |> length() |> expect_equal(1)

  result[[1]]$analyses[[1]]$analysisId |> expect_equal(141)
})
