#' covariateData_YearOfBirth
#'
#' @importFrom DatabaseConnector querySql
#' @importFrom SqlRender render translate
#'
#' @export
#'
covariateData_YearOfBirth <- function() {
  covariateSettings <- list()
  attr(covariateSettings, "fun") <- "HadesExtras::YearOfBirth"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}

#' YearOfBirth
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
YearOfBirth <- function(
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
  writeLines("Constructing YearOfBirth covariate")

  # Some SQL to construct the covariate:
  sql <- paste(
    "SELECT
               cohort_definition_id AS cohort_definition_id,
               @row_id_field AS row_id,
               1041 AS covariate_id,
               p.year_of_birth AS covariate_value",
    "FROM @cohort_table c",
    "INNER JOIN @cdm_database_schema.person p",
    "ON p.person_id = c.subject_id",
    "{@cohort_ids != -1} ? {WHERE cohort_definition_id IN (@cohort_ids)}"
  )
  sql <- SqlRender::render(sql,
    cohort_table = cohortTable,
    cohort_ids = cohortIds,
    row_id_field = rowIdField,
    cdm_database_schema = cdmDatabaseSchema
  )
  sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))

  # Retrieve the covariate:
  covariates <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)

  # Construct covariate reference:
  covariateRef <- data.frame(
    covariateId = 1041,
    covariateName = "Year of birth",
    analysisId = 41,
    conceptId = 0
  )

  # Construct analysis reference:
  analysisRef <- data.frame(
    analysisId = 41,
    analysisName = "YearOfBirth",
    domainId = "Demographics",
    isBinary = "N",
    missingMeansZero = "Y"
  )

  # Construct analysis reference:
  metaData <- list(sql = sql, call = match.call())
  if (!aggregated) {
    result <- Andromeda::andromeda(
      covariates = covariates |> dplyr::select(-cohortDefinitionId),
      covariateRef = covariateRef,
      analysisRef = analysisRef
    )
  } else {
    result <- Andromeda::andromeda(
      covariates = tibble::tibble(covariateId = NA_real_, sumValue = NA_integer_, averageValue = NA_integer_, .rows = 0),
      covariateRef = covariateRef,
      analysisRef = analysisRef,
      covariatesContinuous = covariates |>
        tidyr::nest(data = -cohortDefinitionId) |>
        dplyr::mutate(data = purrr::map(data, .computeStats)) |>
        tidyr::unnest(data)
    )
  }
  attr(result, "metaData") <- metaData
  class(result) <- "CovariateData"

  return(result)
}

# Aggregate continuous variables where missing means missing
.computeStats <- function(data) {
  probs <- c(0, 0.1, 0.25, 0.5, 0.75, 0.9, 1)
  quants <- quantile(data$covariateValue, probs = probs, type = 1)
  result <- tibble(
    covariateId = data$covariateId[1],
    countValue = length(data$covariateValue),
    minValue = quants[1],
    maxValue = quants[7],
    averageValue = mean(data$covariateValue),
    standardDeviation = sd(data$covariateValue),
    medianValue = quants[4],
    p10Value = quants[2],
    p25Value = quants[3],
    p75Value = quants[5],
    p90Value = quants[6]
  )
  return(result)
}


#
# ATC groups
#

#' covariateData_ATCgroups
#'
#' @description Creates covariate settings for ATC drug groups
#' @param temporalStartDays Start day relative to index (-99999 by default)
#' @param temporalEndDays End day relative to index (99999 by default)
#' 
#' @return A covariate settings object for ATC drug groups
#' 
#' @importFrom DatabaseConnector querySql
#' @importFrom SqlRender render translate
#'
#' @export
#'
covariateData_ATCgroups <- function(
    temporalStartDays = -99999,
    temporalEndDays = 99999,
    continuous = FALSE) {
  covariateSettings <- list(
    temporal = TRUE,
    temporalSequence = FALSE,
    temporalStartDays = temporalStartDays,
    temporalEndDays = temporalEndDays,
    continuous = continuous
  )
  attr(covariateSettings, "fun") <- "HadesExtras::ATCgroups"
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
  writeLines("Constructing ATCgroups covariate")

  continuous <- covariateSettings$continuous

  # Some SQL to construct the covariate:
  if (continuous) {
    sql <- SqlRender::readSql(system.file("sql/sql_server/CovariateDDDATCgroups.sql", package = "HadesExtras"))
    analysisId <- 343
  } else {
    sql <- SqlRender::readSql(system.file("sql/sql_server/CovariateATCgroups.sql", package = "HadesExtras"))
    analysisId <- 342
  }

  ATCTimePeriodsValuesStr <- paste0("(", 1:length(covariateSettings$temporalStartDays), ",", covariateSettings$temporalStartDays, ",", covariateSettings$temporalEndDays, ")", collapse = ",")

  sql <- SqlRender::render(sql,
    cdm_database_schema = cdmDatabaseSchema,
    domain_table = "drug_era",
    domain_start_date = "drug_era_start_date",
    domain_end_date = "drug_era_end_date",
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
    analysisName = "ATCgroups",
    domainId = "Drug",
    isBinary = "N",
    missingMeansZero = "Y"
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
