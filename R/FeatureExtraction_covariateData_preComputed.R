#' covariateData_preComputed
#'
#' @description Creates covariate settings for pre-computed covariate analyses.
#' @param preComputedAnalysis A data frame with columns 'analysisType' and 'conceptClassId'
#'
#' @return A covariate settings object for pre-computed analyses
#'
#' @importFrom checkmate assertDataFrame assertSetEqual
#'
#' @export
#'
covariateData_preComputed <- function(
  shcresultsDatabaseSchema, 
  preComputedCovariateTable = "pre_computed_covariate_table",
  covariateGroups,
  covariateTypes
) {
  covariateGroups |> checkmate::assertDataFrame()
  covariateGroups |>
    names() |>
    checkmate::assertSetEqual(c("groupType", "conceptClassId"))

  covariateTypes |> checkmate::assertSubset(c("Binary", "Counts", "AgeFirstEvent", "DaysToFirstEvent"))

  covariateSettings <- list(
    covariateGroups = covariateGroups,
    covariateTypes = covariateTypes
  )
  attr(covariateSettings, "fun") <- "HadesExtras::preComputed"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}

#' ATCgroups
#'
#' @param connection A database connection object created using \code{DatabaseConnector::connect}.
#' @param tempEmulationSchema The temp schema where the covariate tables will be created.
#' @param cdmDatabaseSchema The schema where the cdm tables are located.
#' @param cdmVersion The version of the cdm.
#' @param cohortTable The table where the cohort data is located.
#' @param cohortIds The cohort ids to include.
#' @param rowIdField The field in the cohort table that is the row id.
#' @param covariateSettings A list of settings for the covariate data.
#' @param aggregated Logical. If TRUE, the covariate data is aggregated.
#' @param minCharacterizationMean The minimum mean for the covariate to be included.
#'
#' @importFrom DatabaseConnector querySql
#' @importFrom SqlRender render translate
#' @importFrom dplyr select mutate
#' @importFrom tidyr nest unnest
#' @importFrom purrr map
#' @importFrom Andromeda andromeda
#'
#' @export
#'
preComputed <- function(
  connection,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cdmDatabaseSchema,
  cdmVersion = "5",
  cohortTable = "#cohort_person",
  cohortIds = c(-1),
  rowIdField = "subject_id",
  covariateSettings,
  aggregated = FALSE,
  minCharacterizationMean = 0
) {
  shcresultsDatabaseSchema <- covariateSettings$shcresultsDatabaseSchema
  preComputedCovariateTable <- covariateSettings$preComputedCovariateTable
  covariateGroups <- covariateSettings$covariateGroups
  covariateTypes <- covariateSettings$covariateTypes


  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "pre_computed_covariate_table",
    data = covariateGroups,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE
  )
  


  
  sql <- SqlRender::render(sql,
    cdm_database_schema = cdmDatabaseSchema,
    domain_table = "drug_exposure",
    domain_start_date = "drug_exposure_start_date",
    domain_end_date = "drug_exposure_end_date",
    domain_concept_id = "drug_concept_id",
    analysis_id = analysisId,
    aggregated = aggregated,
    atc_time_period_values = ATCTimePeriodsValuesStr,
    row_id_field = "subject_id",
    cohort_definition_id = paste0(cohortIds, collapse = ","),
    cohort_table = cohortTable
  )
  sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))


  # Construct analysis reference:
  metaData <- list(sql = sql, call = match.call())

  if (continuous) {
    result <- Andromeda::andromeda(
      covariatesContinuous = covariatesContinuous,
      covariateRef = covariateRef,
      analysisRef = analysisRef
    )
  } else {
    result <- Andromeda::andromeda(
      covariates = covariates,
      covariateRef = covariateRef,
      analysisRef = analysisRef
    )
  }

  attr(result, "metaData") <- metaData
  class(result) <- "CovariateData"

  return(result)
}
