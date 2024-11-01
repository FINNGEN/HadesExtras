

#' covariateData_YearOfBirth
#'
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
    minCharacterizationMean = 0
  ) {

  writeLines("Constructing YearOfBirth covariate")

  # Some SQL to construct the covariate:
  sql <- paste("SELECT
               cohort_definition_id AS cohort_definition_id,
               @row_id_field AS row_id,
               1041 AS covariate_id,
               p.year_of_birth AS covariate_value",
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
  if (!aggregated) {
    result <- Andromeda::andromeda(
      covariates = covariates |> dplyr::select(-cohortDefinitionId),
      covariateRef = covariateRef,
      analysisRef = analysisRef
    )
  }else{
    result <- Andromeda::andromeda(
      covariates = tibble::tibble(covariateId=NA_real_, sumValue=NA_integer_, averageValue=NA_integer_, .rows = 0),
      covariateRef = covariateRef,
      analysisRef = analysisRef,
      covariatesContinuous = covariates   |>
        tidyr::nest(data=-cohortDefinitionId)   |>
        dplyr::mutate(data=purrr::map(data,.computeStats)) |>
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











