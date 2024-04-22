


#' Get List of Analysis
#'
#' This function returns a tibble containing analysis information.
#'
#' @importFrom readr cols col_character col_logical read_csv
#'
#' @return A data frame containing analysis information.
#' @export
#'
getListOfAnalysis <- function() {

  colTypes <- readr::cols(
    analysisId = readr::col_integer(),
    analysisName = readr::col_character(),
    domainId = readr::col_character(),
    isBinary = readr::col_logical(),
    isStandard = readr::col_logical(),
    isSourceConcept = readr::col_logical()
    )

  analysisRef <- system.file("covariates/analysisRef.csv", package = "HadesExtras") |>
    readr::read_csv(col_types = colTypes)


  return(analysisRef)
}




#' Create Temporal Covariate Settings From List
#'
#' This function creates temporal covariate settings based on a list of analysis IDs, temporal start days, and temporal end days.
#'
#' @param analysisIds A vector of analysis IDs.
#' @param temporalStartDays A vector of temporal start days for each analysis ID.
#' @param temporalEndDays A vector of temporal end days for each analysis ID.
#'
#' @importFrom checkmate assertInteger subsetVector
#' @importFrom FeatureExtraction createTemporalCovariateSettings createDetailedTemporalCovariateSettings
#' @importFrom dplyr filter nrow
#'
#' @return A list of temporal covariate settings.
#'
#' @export
#'
FeatureExtraction_createTemporalCovariateSettingsFromList  <- function(
    analysisIds,
    temporalStartDays = c(-99999),
    temporalEndDays =   c(99999)
){

  analysisIds |> checkmate::assertNumeric()
  temporalStartDays |> checkmate::assertNumeric()
  temporalEndDays |> checkmate::assertNumeric()

  # check all analsyisids are valid
  analysisRef <- getListOfAnalysis()
  checkmate::assertSubset(analysisIds, analysisRef$analysisId)

  selectedAnalysis <- analysisRef |> dplyr::filter(analysisId %in% analysisIds)

  listOfCovariateSetings <- list()

  #
  # Standard feature extraction settings
  #
  if (selectedAnalysis |> dplyr::filter(isStandard == TRUE ) |> nrow() > 0) {

    standardCovariatesSettings <- FeatureExtraction::createTemporalCovariateSettings(
      useDemographicsGender = ifelse("DemographicsGender" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDemographicsAge = ifelse("DemographicsAge" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDemographicsAgeGroup =  ifelse("DemographicsAgeGroup" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDemographicsRace = ifelse("DemographicsRace" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDemographicsEthnicity = ifelse("DemographicsEthnicity" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDemographicsIndexYear = ifelse("DemographicsIndexYear" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDemographicsIndexMonth = ifelse("DemographicsIndexMonth" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDemographicsPriorObservationTime = ifelse("DemographicsPriorObservationTime" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDemographicsPostObservationTime = ifelse("DemographicsPostObservationTime" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDemographicsTimeInCohort = ifelse("DemographicsTimeInCohort" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDemographicsIndexYearMonth = ifelse("DemographicsIndexYearMonth" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useCareSiteId = ifelse("CareSiteId" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useConditionOccurrence =  ifelse("ConditionOccurrence" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useConditionOccurrencePrimaryInpatient = ifelse("ConditionOccurrencePrimaryInpatient" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useConditionEraStart = ifelse("ConditionEraStart" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useConditionEraOverlap = ifelse("ConditionEraOverlap" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useConditionEraGroupStart = ifelse("ConditionEraGroupStart" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useConditionEraGroupOverlap = ifelse("ConditionEraGroupOverlap" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDrugExposure = ifelse("DrugExposure" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDrugEraStart = ifelse("DrugEraStart" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDrugEraOverlap = ifelse("DrugEraOverlap" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDrugEraGroupStart = ifelse("DrugEraGroupStart" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDrugEraGroupOverlap = ifelse("DrugEraGroupOverlap" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useProcedureOccurrence = ifelse("ProcedureOccurrence" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDeviceExposure = ifelse("DeviceExposure" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useMeasurement = ifelse("Measurement" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useMeasurementValue = ifelse("MeasurementValue" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useMeasurementRangeGroup = ifelse("MeasurementRangeGroup" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useObservation = ifelse("Observation" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useCharlsonIndex =  ifelse("CharlsonIndex" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDcsi = ifelse("Dcsi" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useChads2 = ifelse("Chads2" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useChads2Vasc = ifelse("Chads2Vasc" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useHfrs = ifelse("Hfrs" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDistinctConditionCount = ifelse("DistinctConditionCount" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDistinctIngredientCount = ifelse("DistinctIngredientCount" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDistinctProcedureCount = ifelse("DistinctProcedureCount" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDistinctMeasurementCount = ifelse("DistinctMeasurementCount" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useDistinctObservationCount = ifelse("DistinctObservationCount" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useVisitCount = ifelse("VisitCount" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      useVisitConceptCount = ifelse("VisitConceptCount" %in% selectedAnalysis$analysisName, TRUE, FALSE),
      temporalStartDays = temporalStartDays,
      temporalEndDays =   temporalEndDays

    )

    listOfCovariateSetings[[length(listOfCovariateSetings)+1]] <- standardCovariatesSettings

  }


  #
  # detailed feature extraction settings
  #
  if (selectedAnalysis |> dplyr::filter(isStandard == FALSE ) |> nrow() > 0) {

    listAnalyses <- list()
    if ("ConditionOccurrenceSourceConcept" %in% selectedAnalysis$analysisName) {
      listAnalyses[[length(listAnalyses)+1]] <- analysisDetails_ConditionOccurrenceSourceConcept
    }
    if ("DrugExposureSourceConcept" %in% selectedAnalysis$analysisName) {
      listAnalyses[[length(listAnalyses)+1]] <- analysisDetails_DrugExposureSourceConcept
    }
    if ("ProcedureOccurrenceSourceConcept" %in% selectedAnalysis$analysisName) {
      listAnalyses[[length(listAnalyses)+1]] <-analysisDetails_ProcedureOccurrenceSourceConcept
    }
    if ("MeasurementSourceConcept" %in% selectedAnalysis$analysisName) {
      listAnalyses[[length(listAnalyses)+1]] <- analysisDetails_MeasurementSourceConcept
    }
    if ("DeviceExposureSourceConcept" %in% selectedAnalysis$analysisName) {
      listAnalyses[[length(listAnalyses)+1]] <-analysisDetails_DeviceExposureSourceConcept
    }
    if ("ObservationSourceConcept" %in% selectedAnalysis$analysisName) {
      listAnalyses[[length(listAnalyses)+1]] <- analysisDetails_ObservationSourceConcept
    }

    if (length(listAnalyses) != 0) {
      analysisDetailsCovatiateSettings <- FeatureExtraction::createDetailedTemporalCovariateSettings(
        analyses = listAnalyses,
        temporalStartDays = temporalStartDays,
        temporalEndDays =   temporalEndDays
      )

      listOfCovariateSetings[[length(listOfCovariateSetings)+1]] <- analysisDetailsCovatiateSettings
    }

  }

  #
  # custom covariate settings
  #

  if ("YearOfBirth" %in% selectedAnalysis$analysisName) {
    listOfCovariateSetings[[length(listOfCovariateSetings)+1]] <- covariateData_YearOfBirth()
  }



  return(listOfCovariateSetings)

}
