test_that("preComputed returns correct value", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  personCodeCountsTable <- "person_code_counts_test"

  createPersonCodeCountsTable(cohortTableHandler, personCodeCountsTable = personCodeCountsTable)

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

  preComputedAnalysis <- getListOfPreComputedAnalysis(cohortTableHandler, personCodeCountsTable = personCodeCountsTable)

  covariatesAndromeda <- getPreComputedCovariatesAggregated(
    connection = cohortTableHandler$connectionHandler$getConnection(),
    cdmDatabaseSchema = cohortTableHandler$cdmDatabaseSchema,
    cohortTableSchema = cohortTableHandler$cohortDatabaseSchema,
    cohortTable = cohortTableHandler$cohortTableNames$cohortTable,
    cohortIds = c(1, 2),
    resultsDatabaseSchema = cohortTableHandler$resultsDatabaseSchema,
    personCodeCountsTable = personCodeCountsTable,
    covariateGroups = preComputedAnalysis,
    covariateTypes = c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous"),
    minCharacterizationMean = 0
  )

  Andromeda::saveAndromeda(covariatesAndromeda, file = "covariatesAndromeda.zip")

  #
  # cohortCounts
  #
  cohortCounts <- covariatesAndromeda$cohortCounts |> dplyr::collect()
  cohortCounts |>
    names() |>
    checkmate::assertSubset(c("cohortId", "cohortEntries", "cohortSubjects"))
  rules <- validate::validator(
    cohortId.is.integer = is.numeric(cohortId),
    cohortId.not.missing = !is.na(cohortId),
    cohortId.is.positive = cohortId > 0,
    #
    cohortEntries.is.integer = is.numeric(cohortEntries),
    cohortEntries.not.missing = !is.na(cohortEntries),
    cohortEntries.is.positive = cohortEntries > 0,
    #
    cohortSubjects.is.integer = is.numeric(cohortSubjects),
    cohortSubjects.not.missing = !is.na(cohortSubjects),
    cohortSubjects.is.positive = cohortSubjects > 0,
    # multicolumnar validation
    key.is.unique = is_unique(cohortId)
  )
  results <- validate::confront(cohortCounts, rules)
  validate::summary(results) |>
    dplyr::filter(fails > 0) |>
    nrow() |>
    expect_equal(0)

  #
  # analysisRef
  #
  analysisRef <- covariatesAndromeda$analysisRef |> dplyr::collect()
  analysisRef |>
    names() |>
    checkmate::assertSubset(c("analysisId", "analysisType", "analysisGroupId", "domainId"))
  rules <- validate::validator(
    analysisId.is.integer = is.numeric(analysisId),
    analysisId.not.missing = !is.na(analysisId),
    analysisId.is.unique = is_unique(analysisId),
    #
    analysisType.not.missing = !is.na(analysisType),
    analysisType.is.character = is.character(analysisType),
    analysisType.is.subset = analysisType %in% c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous"),
    #
    analysisGroupId.not.missing = !is.na(analysisGroupId),
    analysisGroupId.is.integer = is.numeric(analysisGroupId),
    #
    domainId.not.missing = !is.na(domainId),
    domainId.is.character = is.character(domainId),
    # multicolumnar validation
    key.is.unique = is_unique(analysisId, analysisType, analysisGroupId, domainId)
  )
  results <- validate::confront(analysisRef, rules)
  validate::summary(results)$fails |>
    sum() |>
    expect_equal(0)

  #
  # conceptRef
  #
  conceptRef <- covariatesAndromeda$conceptRef |> dplyr::collect()
  conceptRef |>
    names() |>
    checkmate::assertSubset(c("conceptId", "conceptCode", "conceptClassId", "conceptName", "vocabularyId"))
  rules <- validate::validator(
    conceptId.is.integer = is.numeric(conceptId),
    conceptId.not.missing = !is.na(conceptId),
    conceptId.is.unique = is_unique(conceptId),
    #
    conceptCode.not.missing = !is.na(conceptCode),
    conceptCode.is.character = is.character(conceptCode),
    #
    conceptClassId.not.missing = !is.na(conceptClassId),
    conceptClassId.is.character = is.character(conceptClassId),
    #
    conceptName.not.missing = !is.na(conceptName),
    conceptName.is.character = is.character(conceptName)
  )
  results <- validate::confront(conceptRef, rules)
  validate::summary(results)$fails |>
    sum() |>
    expect_equal(0)

  #
  # covariates
  #
  covariates <- covariatesAndromeda$covariates |> dplyr::collect()
  covariates |>
    names() |>
    checkmate::assertSubset(c("cohortDefinitionId", "analysisId", "conceptId", "categoryId", "sumValue"))
  covariates <- covariates |> dplyr::left_join(cohortCounts |> dplyr::select(cohortId, cohortSubjects), by = c("cohortDefinitionId"="cohortId"))
  rules <- validate::validator(
    cohortDefinitionId.is.integer = is.numeric(cohortDefinitionId),
    cohortDefinitionId.not.missing = !is.na(cohortDefinitionId),
    cohortDefinitionId.is.positive = cohortDefinitionId > 0,
    #
    analysisId.is.integer = is.numeric(analysisId),
    analysisId.not.missing = !is.na(analysisId),
    analysisId.is.positive = analysisId > 0,
    #
    conceptId.is.integer = is.numeric(conceptId),
    conceptId.not.missing = !is.na(conceptId),
    conceptId.is.positive = conceptId > 0,
    #
    categoryId.is.integer = is.numeric(categoryId),
    categoryId.not.missing.for.categorical = (analysisId == 265 & !is.na(categoryId) | (analysisId != 265 & is.na(categoryId))),
    #
    sumValue.is.integer = is.numeric(sumValue),
    sumValue.not.missing = !is.na(sumValue),
    sumValue.is.positive = sumValue > 0,
    sumValue.is.less.than.or.equal.to.cohortSubjects = sumValue <= cohortSubjects,
    # multicolumnar validation
    key.is.unique = is_unique(cohortDefinitionId, analysisId, conceptId, categoryId)
  )
  results <- validate::confront(covariates, rules)
  validate::summary(results) |>
    dplyr::filter(fails > 0) |>
    nrow() |>
    expect_equal(0)

  covariates |> dplyr::filter(sumValue > cohortSubjects) |> print(n = Inf)

  # measurements: binary and aggregated categorical should have the same sumValue
  covBinaryMeasurements <- covariates |>
    dplyr::filter(analysisId == 165 & is.na(categoryId)) |>
    dplyr::select(cohortDefinitionId, conceptId, sumValueBinary = sumValue)
  covCategoricalMeasurements <- covariates |>
    dplyr::filter(analysisId == 265 & !is.na(categoryId)) |>
    dplyr::group_by(cohortDefinitionId, conceptId) |>
    dplyr::summarize(sumValueCategorical = sum(sumValue))
  dplyr::inner_join(covBinaryMeasurements, covCategoricalMeasurements, by = c("cohortDefinitionId", "conceptId")) |>
    dplyr::filter(sumValueBinary != sumValueCategorical) |>
    nrow() |>
    expect_equal(0)

  #
  # covariatesContinuous
  #
  covariatesContinuous <- covariatesAndromeda$covariatesContinuous |> dplyr::collect()
  covariatesContinuous |>
    names() |>
    checkmate::assertSubset(c("cohortDefinitionId", "analysisId", "conceptId", "countValue", "minValue", "maxValue", "averageValue", "standardDeviation", "medianValue", "p10Value", "p25Value", "p75Value", "p90Value", "unit"))
  covariatesContinuous <- covariatesContinuous |> dplyr::left_join(analysisRef |> dplyr::select(analysisId, analysisType), by = c("analysisId" = "analysisId"))
  rules <- validate::validator(
    analysisType.not.missing = !is.na(analysisType),
    analysisType.is.character = is.character(analysisType),
    analysisType.is.subset = analysisType %in% c("Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous"),
    #
    cohortDefinitionId.is.integer = is.numeric(cohortDefinitionId),
    cohortDefinitionId.not.missing = !is.na(cohortDefinitionId),
    cohortDefinitionId.is.positive = cohortDefinitionId > 0,
    #
    analysisId.is.integer = is.numeric(analysisId),
    analysisId.not.missing = !is.na(analysisId),
    analysisId.is.positive = analysisId > 0,
    #
    conceptId.is.integer = is.numeric(conceptId),
    conceptId.not.missing = !is.na(conceptId),
    conceptId.is.positive = conceptId > 0,
    #
    countValue.is.integer = is.numeric(countValue),
    countValue.not.missing = !is.na(countValue),
    countValue.is.positive = countValue > 0,
    #
    minValue.is.numeric = is.numeric(minValue),
    minValue.not.missing = !is.na(minValue),
    minValue.is.positive.for.counts = (analysisType == "Counts" & minValue > 0) | (analysisType != "Counts"),
    #
    maxValue.is.numeric = is.numeric(maxValue),
    maxValue.not.missing = !is.na(maxValue),
    maxValue.is.positive.for.counts = (analysisType == "Counts" & maxValue > 0) | (analysisType != "Counts"),
    # multicolumnar validation
    key.is.unique = is_unique(cohortDefinitionId, analysisId, conceptId, unit)
  )
  results <- validate::confront(covariatesContinuous, rules)
  validate::summary(results) |>
    dplyr::filter(fails > 0) |>
    nrow() |>
    expect_equal(0)
})
