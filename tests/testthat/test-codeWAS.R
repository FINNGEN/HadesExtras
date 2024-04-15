
test_that("executeCodeWAS works", {

  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  # cohorts from eunomia
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/asthma/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/asthma/cohorts"),
    sqlFolder = here::here("inst/testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/asthma/cohort_subset_definitions/"),
    #packageName = "HadesExtras",
    verbose = FALSE
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  temporalStartDays = c( -999999 )
  temporalEndDays =   c( 999999)

  temporalCovariateSettings = FeatureExtraction::createTemporalCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useConditionOccurrence = TRUE,
    useDrugExposure =  TRUE,
    useProcedureOccurrence = TRUE,
    useMeasurement = TRUE,
    useObservation = TRUE,
    temporalStartDays = temporalStartDays,
    temporalEndDays =   temporalEndDays
  )


  exportFolder <- file.path(tempdir(), "CodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({unlink(exportFolder, recursive = TRUE);gc()})

  executeCodeWAS(
    exportFolder = exportFolder,
    cohortTableHandler = cohortTableHandler,
    cohortIdCases = 1,
    cohortIdControls = 2,
    covariateSettings = temporalCovariateSettings,
    minCellCount = 1
  )

  expect_true(file.exists(file.path(exportFolder, "temporal_covariate_CodeWAS.csv")))
})




test_that("executeCodeWAS works for cohort start date outside of observation paeriod", {

  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  # cohorts from eunomia
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/asthma/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/asthma/cohorts"),
    sqlFolder = here::here("inst/testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/asthma/cohort_subset_definitions/"),
    #packageName = "HadesExtras",
    verbose = FALSE
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  # Match to sex and bday, match ratio 10
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 1,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 1,
        matchRatio = 10,
        matchSex = TRUE,
        matchBirthYear = TRUE,
        matchCohortStartDateWithInDuration = FALSE,
        newCohortStartDate = "asMatch",
        newCohortEndDate = "keep"
      )
    )
  )

  cohortDefinitionSetWithSubsetDef <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 2)

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSetWithSubsetDef)

  # change one person
  DatabaseConnector::executeSql(
    connection= cohortTableHandler$connectionHandler$getConnection(),
    sql = "UPDATE main.observation_period SET observation_period_start_date = '1984-07-26' WHERE person_id = 9" )

  temporalStartDays = c(   -365*2, -365*1, 0,     1,   365+1 )
  temporalEndDays =   c( -365*1-1,     -1, 0, 365*1,   365*2)

  temporalCovariateSettings = FeatureExtraction::createTemporalCovariateSettings(
    useConditionOccurrence = TRUE,
    useDrugExposure =  TRUE,
    useProcedureOccurrence = TRUE,
    useMeasurement = TRUE,
    useObservation = TRUE,
    temporalStartDays = temporalStartDays,
    temporalEndDays =   temporalEndDays
  )


  exportFolder <- file.path(tempdir(), "CodeWAS")
  dir.create(exportFolder, showWarnings = FALSE)
  on.exit({unlink(exportFolder, recursive = TRUE);gc()})

  executeCodeWAS(
    exportFolder = exportFolder,
    cohortTableHandler = cohortTableHandler,
    cohortIdCases = 1,
    cohortIdControls = 2001,
    covariateSettings = temporalCovariateSettings,
    minCellCount = 1
  )

  expect_true(file.exists(file.path(exportFolder, "temporal_covariate_CodeWAS.csv")))
})


