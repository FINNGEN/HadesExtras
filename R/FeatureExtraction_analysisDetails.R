

#' Create Temporal Source Covariate Settings
#'
#' This function generates settings for temporal source covariates to be used in feature extraction, DEPRECATED use FeatureExtraction_createTemporalCovariateSettingsFromList.
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
    if(useConditionOccurrenceSourceConcept) analysisDetails_ConditionOccurrenceSourceConcept,
    if(useDrugExposureSourceConcept) analysisDetails_DrugExposureSourceConcept,
    if(useProcedureOccurrenceSourceConcept) analysisDetails_ProcedureOccurrenceSourceConcept,
    if(useMeasurementSourceConcept) analysisDetails_MeasurementSourceConcept,
    if(useDeviceExposureSourceConcept) analysisDetails_DeviceExposureSourceConcept,
    if(useObservationSourceConcept) analysisDetails_ObservationSourceConcept
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

analysisDetails_ConditionOccurrenceSourceConcept <- FeatureExtraction::createAnalysisDetails(
  analysisId = 141,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 141,
    analysisName = "ConditionOccurrenceSourceConcept",
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

analysisDetails_DrugExposureSourceConcept <- FeatureExtraction::createAnalysisDetails(
  analysisId = 341,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 341,
    analysisName = "DrugExposureSourceConcept",
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

analysisDetails_ProcedureOccurrenceSourceConcept <- FeatureExtraction::createAnalysisDetails(
  analysisId = 541,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 514,
    analysisName = "ProcedureOccurrenceSourceConcept",
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

analysisDetails_DeviceExposureSourceConcept <- FeatureExtraction::createAnalysisDetails(
  analysisId = 641,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 641,
    analysisName = "DeviceExposureSourceConcept",
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

analysisDetails_MeasurementSourceConcept <- FeatureExtraction::createAnalysisDetails(
  analysisId = 741,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 741,
    analysisName = "MeasurementSourceConcept",
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

analysisDetails_ObservationSourceConcept <- FeatureExtraction::createAnalysisDetails(
  analysisId = 841,
  sqlFileName = "DomainConcept.sql",
  parameters = list(
    analysisId = 841,
    analysisName = "ObservationSourceConcept",
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
