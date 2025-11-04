
#' covariateData_ATCgroups
#'
#' @description Creates covariate settings for ATC drug groups
#' @param temporalStartDays Start day relative to index (-99999 by default)
#' @param temporalEndDays End day relative to index (99999 by default)
#' @param continuous Logical. If TRUE, the covariate data is continuous.
#' 
#' @return A covariate settings object for ATC drug groups
#' 
#' @importFrom DatabaseConnector querySql
#' @importFrom SqlRender render translate
#'
#' @export
#'
covariateData_preComputed <- function(
  analysisTypes 
) {
  covariateSettings <- list(
    analysisTypes = analysisTypes
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
ATCgroups <- function(
    connection,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    cdmDatabaseSchema,
    cdmVersion = "5",
    cohortTable = "#cohort_person",
    cohortIds = c(-1),
    rowIdField = "subject_id",
    covariateSettings,
    aggregated = FALSE,
    minCharacterizationMean = 0) {

  continuous <- covariateSettings$continuous

  # Some SQL to construct the covariate:
  if (continuous) {
    sql <- SqlRender::readSql(system.file("sql/sql_server/CovariateDDDATCgroups.sql", package = "HadesExtras"))
    analysisId <- 343
    analysisName <- "DDDATCgroups"
    isBinary <- "Y"
    missingMeansZero <- "N"
  } else {
    sql <- SqlRender::readSql(system.file("sql/sql_server/CovariateATCgroups.sql", package = "HadesExtras"))
    analysisId <- 342
    analysisName <- "ATCgroups"
    isBinary <- "N"
    missingMeansZero <- "Y"
  }

  writeLines(paste0("Constructing ", analysisName, " covariate"))

  ATCTimePeriodsValuesStr <- paste0("(", 1:length(covariateSettings$temporalStartDays), ",", covariateSettings$temporalStartDays, ",", covariateSettings$temporalEndDays, ")", collapse = ",")

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

  # Retrieve the covariate:
  DatabaseConnector::executeSql(connection, sql)

  # Construct covariate reference:
  if (continuous) {
    covariatesContinuous <- DatabaseConnector::dbReadTable(connection, "#atc_ddd_covariate_table")  |> 
    SqlRender::snakeCaseToCamelCaseNames()
    covariateRef <- DatabaseConnector::dbReadTable(connection, "#atc_ddd_covariate_ref")  |> 
    SqlRender::snakeCaseToCamelCaseNames()
  } else {
    covariates <- DatabaseConnector::dbReadTable(connection, "#atc_covariate_table")  |> 
    SqlRender::snakeCaseToCamelCaseNames()
    covariateRef <- DatabaseConnector::dbReadTable(connection, "#atc_covariate_ref")  |> 
    SqlRender::snakeCaseToCamelCaseNames()
  }
  

  # Construct analysis reference:
  analysisRef <- data.frame(
    analysisId = analysisId,
    analysisName = analysisName,
    domainId = "Drug",
    isBinary = isBinary,
    missingMeansZero = missingMeansZero
  )

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