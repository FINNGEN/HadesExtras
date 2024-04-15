library(tidyverse)
renv::install('PheWAS/PheWAS')

cohortTableHandler <- helper_createNewCohortTableHandler()

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

covariateSettings = FeatureExtraction::createTemporalCovariateSettings(
  useDemographicsGender = TRUE,
  useDemographicsAge = TRUE,
  useDemographicsAgeGroup = TRUE,
  useDemographicsRace = TRUE,
  useDemographicsEthnicity = TRUE,
  useDemographicsIndexYear = TRUE,
  useDemographicsIndexMonth = TRUE,
  useDemographicsPriorObservationTime = TRUE,
  useDemographicsPostObservationTime = TRUE,
  useDemographicsTimeInCohort = TRUE,
  useDemographicsIndexYearMonth = TRUE,
  useCareSiteId = TRUE,
  useConditionOccurrence = TRUE,
  useConditionOccurrencePrimaryInpatient = TRUE,
  useConditionEraStart = TRUE,
  useConditionEraOverlap = TRUE,
  useConditionEraGroupStart = TRUE,
  useConditionEraGroupOverlap = TRUE,
  useDrugExposure = TRUE,
  useDrugEraStart = TRUE,
  useDrugEraOverlap = TRUE,
  useDrugEraGroupStart = TRUE,
  useDrugEraGroupOverlap = TRUE,
  useProcedureOccurrence = TRUE,
  useDeviceExposure = TRUE,
  useMeasurement = TRUE,
  useMeasurementValue = TRUE,
  useMeasurementRangeGroup = TRUE,
  useObservation = TRUE,
  useCharlsonIndex = TRUE,
  useDcsi = TRUE,
  useChads2 = TRUE,
  useChads2Vasc = TRUE,
  useHfrs = TRUE,
  useDistinctConditionCount = TRUE,
  useDistinctIngredientCount = TRUE,
  useDistinctProcedureCount = TRUE,
  useDistinctMeasurementCount = TRUE,
  useDistinctObservationCount = TRUE,
  useVisitCount = TRUE,
  useVisitConceptCount = TRUE,
  temporalStartDays = temporalStartDays,
  temporalEndDays =   temporalEndDays
)

cohortIdCases = 1
cohortIdControls = 2

connection <- cohortTableHandler$connectionHandler$getConnection()
cohortTable <- cohortTableHandler$cohortTableNames$cohortTable
cdmDatabaseSchema <- cohortTableHandler$cdmDatabaseSchema
cohortDatabaseSchema <- cohortTableHandler$cohortDatabaseSchema
vocabularyDatabaseSchema <- cohortTableHandler$vocabularyDatabaseSchema
cohortDefinitionSet <- cohortTableHandler$cohortDefinitionSet
databaseId <- cohortTableHandler$databaseName
databaseName <- cohortTableHandler$CDMInfo$cdm_source_abbreviation
databaseDescription <- cohortTableHandler$CDMInfo$cdm_source_name
vocabularyVersionCdm <- cohortTableHandler$CDMInfo$cdm_version
vocabularyVersion <- cohortTableHandler$vocabularyInfo$vocabulary_version

ParallelLogger::logInfo("Getting FeatureExtraction for cases")
covariate_case <- FeatureExtraction::getDbCovariateData(
  connection = connection,
  cohortTable = cohortTable,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  covariateSettings = covariateSettings,
  cohortId = cohortIdCases,
  aggregated = F
)
ParallelLogger::logInfo("Getting FeatureExtraction for controls")
covariate_control <- FeatureExtraction::getDbCovariateData(
  connection = connection,
  cohortTable = cohortTable,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  covariateSettings = covariateSettings,
  cohortId = cohortIdControls,
  aggregated = F
)


covarCase  <- covariate_case$covariates |>
  dplyr::left_join(covariate_case$covariateRef, by = "covariateId") |>
  dplyr::left_join(covariate_case$analysisRef, by = "analysisId")  |>
  dplyr::transmute(
    personId = rowId,
    covariateId = covariateId,
    covariateValue = covariateValue,
    isBinary = isBinary
  ) |> tibble::as_tibble()

covarControl  <- covariate_control$covariates |>
  dplyr::left_join(covariate_control$covariateRef, by = "covariateId") |>
  dplyr::left_join(covariate_control$analysisRef, by = "analysisId")  |>
  dplyr::transmute(
    personId = rowId,
    covariateId = covariateId,
    covariateValue = covariateValue,
    isBinary = isBinary
  ) |>
  tibble::as_tibble() |>
  dplyr::anti_join(covarCase, by = "personId")

# case control covariates
caseControlCovar <- dplyr::bind_rows(
  covarCase |>dplyr::mutate(personId=personId, covariateId="caseControl", covariateValue=TRUE) ,
  covarControl |> dplyr::mutate(personId=personId, covariateId="caseControl", covariateValue=FALSE)
)  |>
  dplyr::select(-isBinary) |>
  dplyr::distinct() |>
  tidyr::spread(covariateId, covariateValue)


# binary covariates
binaryCovar <- dplyr::bind_rows(
  covarCase ,
  covarControl
)  |>
  dplyr::filter(isBinary=='Y') |>
  dplyr::select(personId, covariateId, covariateValue) |>
  dplyr::distinct() |>
  dplyr::mutate(covariateValue = ifelse(covariateValue == 1, TRUE, FALSE)) |>
  tidyr::spread(covariateId, covariateValue) |>
  dplyr::mutate(dplyr::across(dplyr::everything(), ~dplyr::if_else(is.na(.), FALSE, .) ))


# continuous covariates
continuousCovar <- dplyr::bind_rows(
  covarCase ,
  covarControl
)  |>
  dplyr::filter(isBinary!='Y') |>
  dplyr::select(personId, covariateId, covariateValue) |>
  dplyr::distinct() |>
  tidyr::spread(covariateId, covariateValue)



pheno <- caseControlCovar |>
  dplyr::left_join(binaryCovar, by = "personId") |>
  dplyr::left_join(continuousCovar, by = "personId")


phenotypes  <- pheno  |> select(3:ncol(pheno))  |> colnames()
genotypes <- pheno  |> select(2)  |> colnames()
covariates  <- c( '1002')

phenotypes  <- phenotypes  |> setdiff(covariates)

a2 <- PheWAS::phewas(
  phenotypes = phenotypes,
  genotypes = genotypes,
  data = pheno |> as.data.frame(),
  #
  additive.genotypes = FALSE,
  min.records = 0
)

a2  |>  tibble::as_tibble()

# export

# code: character value uniquely denoting a given phenotype
# OR: Odds ratio from statistical test associating the given phenotype code with the biomarker of interest.
# p_val: The p-value associated with same test.
# description: Short description in words of what the code represents (E.g. "Heart Failure").
# category: A hierarchical category denoting some grouping structure in your phenotypes. For instance all codes related to 'infectious diseases'. These categories are used in coloring the manhattan plot.

phewasResults <- a2$phewasResults |>
  mutate(
    code = as.character(code),
    OR = as.numeric(OR),
    p_val = as.numeric(p_val),
    description = as.character(description),
    category = as.character(category)
  )



