

#' Create Temporal Source Covariate Settings
#'
#' This function generates settings for temporal source covariates to be used in feature extraction.
#'
#' @param useConditionOccurrenceSourceConcept Logical indicating whether to include condition occurrence source concepts.
#' @param useDrugExposureSourceConcept Logical indicating whether to include drug exposure source concepts.
#' @param useProcedureOccurrenceSourceConcept Logical indicating whether to include procedure occurrence source concepts.
#' @param useMeasurementSourceConcept Logical indicating whether to include measurement source concepts.
#' @param useDeviceExposureSourceConcept Logical indicating whether to include device exposure source concepts.
#' @param useObservationSourceConcept Logical indicating whether to include observation source concepts.
#' @param temporalStartDays Vector of integers representing the start days of the temporal window for covariate extraction.
#' @param temporalEndDays Vector of integers representing the end days of the temporal window for covariate extraction.
#'
#' @return Settings for temporal source covariates suitable for feature extraction.
#'
#' @details This function generates settings for temporal source covariates, which can be used in subsequent feature extraction tasks. It allows specifying which types of source concepts to include and the temporal window for extraction.
#'
#' @importFrom FeatureExtraction createDetailedTemporalCovariateSettings
#' @importFrom checkmate assertLogical assertNumeric assertTRUE
#'
#' @export
#'
FeatureExtraction_createTemporalSourceCovariateSettings <- function(
    useConditionOccurrenceSourceConcept = TRUE,
    useDrugExposureSourceConcept =  TRUE,
    useProcedureOccurrenceSourceConcept = TRUE,
    useMeasurementSourceConcept = TRUE,
    useDeviceExposureSourceConcept = TRUE,
    useObservationSourceConcept = TRUE,
    temporalStartDays = -365:-1,
    temporalEndDays =   -365:-1
  ) {

  checkmate::assertLogical(useConditionOccurrenceSourceConcept)
  checkmate::assertLogical(useDrugExposureSourceConcept)
  checkmate::assertLogical(useProcedureOccurrenceSourceConcept)
  checkmate::assertLogical(useMeasurementSourceConcept)
  checkmate::assertLogical(useDeviceExposureSourceConcept)
  checkmate::assertLogical(useObservationSourceConcept)
  checkmate::assertNumeric(temporalStartDays)
  checkmate::assertNumeric(temporalEndDays)

  checkmate::assertTRUE(useConditionOccurrenceSourceConcept | useDrugExposureSourceConcept | useProcedureOccurrenceSourceConcept | useMeasurementSourceConcept | useDeviceExposureSourceConcept | useObservationSourceConcept)

  listAnalyses <- list(
    if(useConditionOccurrenceSourceConcept) analysisDetails_ConditionOccurrenceConceptSource,
    if(useDrugExposureSourceConcept) analysisDetails_DrugExposureConceptSource,
    if(useProcedureOccurrenceSourceConcept) analysisDetails_ProcedureOccurrenceConceptSource,
    if(useMeasurementSourceConcept) analysisDetails_MeasurementConceptSource,
    if(useDeviceExposureSourceConcept) analysisDetails_DeviceExposureConceptSource,
    if(useObservationSourceConcept) analysisDetails_ObservationConceptSource
  )

  settings <- FeatureExtraction::createDetailedTemporalCovariateSettings(
    analyses = listAnalyses,
    temporalStartDays = temporalStartDays,
    temporalEndDays =   temporalEndDays
  )

  return(settings)

}


############################################
# Domain functions looking at source codes #
############################################

analysisDetails_ConditionOccurrenceConceptSource <- FeatureExtraction::createAnalysisDetails(
  analysisId = 103,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 103,
    analysisName = "Condition Occurrence Concept Source",
    domainId = "Condition",
    domainTable = "condition_occurrence",
    domainConceptId = "condition_source_concept_id",
    domain_start_date = "condition_start_date",
    domain_end_date = "condition_end_date"
  ),
  includedCovariateConceptIds = c(),
  addDescendantsToInclude = FALSE,
  excludedCovariateConceptIds = c(),
  addDescendantsToExclude = FALSE,
  includedCovariateIds = c()
)

analysisDetails_DrugExposureConceptSource <- FeatureExtraction::createAnalysisDetails(
  analysisId = 302,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 302,
    analysisName = "Drug Exposure Concept Source",
    domainId = "Drug",
    domainTable = "drug_exposure",
    domainConceptId = "drug_source_concept_id",
    domain_start_date = "drug_exposure_start_date",
    domain_end_date = "drug_exposure_end_date"
  ),
  includedCovariateConceptIds = c(),
  addDescendantsToInclude = FALSE,
  excludedCovariateConceptIds = c(),
  addDescendantsToExclude = FALSE,
  includedCovariateIds = c()
)

analysisDetails_ProcedureOccurrenceConceptSource <- FeatureExtraction::createAnalysisDetails(
  analysisId = 502,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 502,
    analysisName = "Procedure Occurrence Concept Source",
    domainId = "Procedure",
    domainTable = "procedure_occurrence",
    domainConceptId = "procedure_source_concept_id",
    domain_start_date = "procedure_date",
    domain_end_date = "procedure_date"
  ),
  includedCovariateConceptIds = c(),
  addDescendantsToInclude = FALSE,
  excludedCovariateConceptIds = c(),
  addDescendantsToExclude = FALSE,
  includedCovariateIds = c()
)

analysisDetails_DeviceExposureConceptSource <- FeatureExtraction::createAnalysisDetails(
  analysisId = 602,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 602,
    analysisName = "Device Occurrence Concept Source",
    domainId = "Device",
    domainTable = "device_exposure",
    domainConceptId = "device_concept_id",
    domain_start_date = "device_exposure_start_date",
    domain_end_date = "device_exposure_end_date"
  ),
  includedCovariateConceptIds = c(),
  addDescendantsToInclude = FALSE,
  excludedCovariateConceptIds = c(),
  addDescendantsToExclude = FALSE,
  includedCovariateIds = c()
)

analysisDetails_MeasurementConceptSource <- FeatureExtraction::createAnalysisDetails(
  analysisId = 702,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 702,
    analysisName = "Measurement Concept Source",
    domainId = "Measurement",
    domainTable = "measurement",
    domainConceptId = "measurement_concept_id",
    domain_start_date = "measurement_date",
    domain_end_date = "measurement_date"
  ),
  includedCovariateConceptIds = c(),
  addDescendantsToInclude = FALSE,
  excludedCovariateConceptIds = c(),
  addDescendantsToExclude = FALSE,
  includedCovariateIds = c()
)

analysisDetails_ObservationConceptSource <- FeatureExtraction::createAnalysisDetails(
  analysisId = 802,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 802,
    analysisName = "Observation Concept Source",
    domainId = "Observation",
    domainTable = "observation",
    domainConceptId = "observation_source_concept_id",
    domain_start_date = "observation_date",
    domain_end_date = "observation_date"
  ),
  includedCovariateConceptIds = c(),
  addDescendantsToInclude = FALSE,
  excludedCovariateConceptIds = c(),
  addDescendantsToExclude = FALSE,
  includedCovariateIds = c()
)


#############################################################
# Standard functions repeated for the temporal covariates   #
#############################################################

useVisitConceptCount <- FeatureExtraction::createAnalysisDetails(
  analysisId = 911,
  sqlFileName = "ConceptCounts.sql",
  parameters = list(
    analysisId = 911,
    analysisName = "Visit Concept Count",
    domainId = "Visit",
    domainTable = "visit_occurrence",
    domainConceptId = "visit_concept_id",
    domain_start_date = "visit_start_date",
    domain_end_date = "visit_end_date",
    subType = "stratified",
    temporal = TRUE
  ),
  includedCovariateConceptIds = c(),
  addDescendantsToInclude = FALSE,
  excludedCovariateConceptIds = c(),
  addDescendantsToExclude = FALSE,
  includedCovariateIds = c()
)


useConditionEraGroupStart <- FeatureExtraction::createAnalysisDetails(
  analysisId = 203,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 203,
    analysisName = "Condition Era Group Start",
    domainId = "Condition",
    domainTable = "condition_era",
    domainConceptId = "condition_concept_id",
    domain_start_date = "condition_era_start_date",
    domain_end_date = "condition_era_end_date"
  ),
  includedCovariateConceptIds = c(),
  addDescendantsToInclude = FALSE,
  excludedCovariateConceptIds = c(),
  addDescendantsToExclude = FALSE,
  includedCovariateIds = c()
)

useDrugEraGroupStart <- FeatureExtraction::createAnalysisDetails(
  analysisId = 403,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 403,
    analysisName = "Drug Era Group Start",
    domainId = "Drug",
    domainTable = "drug_era",
    domainConceptId = "drug_concept_id",
    domain_start_date = "drug_era_start_date",
    domain_end_date = "drug_era_end_date"
  ),
  includedCovariateConceptIds = c(),
  addDescendantsToInclude = FALSE,
  excludedCovariateConceptIds = c(),
  addDescendantsToExclude = FALSE,
  includedCovariateIds = c()
)

