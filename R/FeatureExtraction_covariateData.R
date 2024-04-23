

#' covariateData_YearOfBirth
#'
#'
#' @export
#'
covariateData_YearOfBirth <- function() {
  covariateSettings <- list()
  attr(covariateSettings, "fun") <- "YearOfBirth"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}

#' YearOfBirth
#'
#'
#' @export
#'
YearOfBirth <- function(
    connection,
    oracleTempSchema = NULL,
    cdmDatabaseSchema,
    cdmVersion = "5",
    cohortTable = "#cohort_person",
    cohortIds = c(-1),
    rowIdField = "subject_id",
    covariateSettings,
    aggregated = FALSE) {

  writeLines("Constructing YearOfBirth covariate")
  if (aggregated) {
    stop("Aggregation not supported")
  }

  # Some SQL to construct the covariate:
  sql <- paste("SELECT @row_id_field AS row_id, 1041 AS covariate_id,",
               "p.year_of_birth",
               "AS covariate_value",
               "FROM @cohort_table c",
               "INNER JOIN @cdm_database_schema.person p",
               "ON p.person_id = c.subject_id",
               "{@cohort_ids != -1} ? {WHERE cohort_definition_id IN (@cohort_ids)}")
  sql <- SqlRender::render(sql,
                           cohort_table = cohortTable,
                           cohort_ids = cohortIds,
                           row_id_field = rowIdField,
                           cdm_database_schema = cdmDatabaseSchema)
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
  result <- Andromeda::andromeda(
    covariates = covariates,
    covariateRef = covariateRef,
    analysisRef = analysisRef
  )
  attr(result, "metaData") <- metaData
  class(result) <- "CovariateData"
  return(result)
}




















