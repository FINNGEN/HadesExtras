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
  resultsDatabaseSchema,
  personCodeCountsTable = "person_code_counts",
  covariateGroups,
  covariateTypes
) {
  covariateGroups |> checkmate::assertDataFrame()
  covariateGroups |>
    names() |>
    checkmate::assertSetEqual(c("analysisGroup", "conceptClassId"))

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
  resultsDatabaseSchema <- covariateSettings$resultsDatabaseSchema
  personCodeCountsTable <- covariateSettings$personCodeCountsTable
  covariateGroups <- covariateSettings$covariateGroups
  covariateTypes <- covariateSettings$covariateTypes

  covariateGroups <- covariateGroups |>
    dplyr::group_by(analysisGroup) |>
    dplyr::mutate(analysisGroupId = dplyr::cur_group_id()) |>
    dplyr::ungroup()
  browser()
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "covariate_groups",
    data = covariateGroups,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE,
    camelCaseToSnakeCase = TRUE
  )

  # Create pre_computed_cohort
  sql <- "
    IF OBJECT_ID('tempdb..#pre_computed_cohort', 'U') IS NOT NULL
    DROP TABLE #pre_computed_cohort;

    -- Cohort patient level data
    SELECT
      c.cohort_definition_id AS cohort_definition_id,
      ccg.analysis_group_id AS analysis_group_id,
      ccg.concept_class_id AS concept_class_id,
      pcct.concept_id AS concept_id,
      c.subject_id AS subject_id,
      pcct.n_records AS n_records,
      DATEDIFF(DAY, c.cohort_start_date, pcct.first_date) AS day_to_first_event,
      pcct.first_age AS age_first_event,
      pcct.aggregated_value AS aggregated_value,
      pcct.aggregated_value_unit AS aggregated_value_unit,
      pcct.aggregated_category AS aggregated_category
    INTO #pre_computed_cohort
    FROM @cohort_table c
    INNER JOIN @results_database_schema.@person_code_counts_table AS pcct ON c.subject_id = pcct.person_id
    INNER JOIN #covariate_groups ccg ON pcct.analysis_group = ccg.analysis_group AND pcct.concept_class_id = ccg.concept_class_id
    {@cohort_definition_id != -1} ? { WHERE c.cohort_definition_id IN (@cohort_definition_id)};
  "

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    results_database_schema = resultsDatabaseSchema,
    person_code_counts_table = personCodeCountsTable,
    cohort_definition_id = paste0(cohortIds, collapse = ","),
    cohort_table = cohortTable
  )

  # Create pre_computed_covariate_table and pre_computed_covariate_ref
  # For each type
  sqlPath <- system.file("sql", "sql_server", "CovariatePreComputed.sql", package = "HadesExtras")
  sql <- SqlRender::readSql(sqlPath)
  for (covariateType in covariateTypes) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      cdm_database_schema = cdmDatabaseSchema,
      results_database_schema = resultsDatabaseSchema,
      person_code_counts_table = personCodeCountsTable,
      cohort_definition_id = paste0(cohortIds, collapse = ","),
      cohort_table = cohortTable,
      row_id_field = rowIdField,
      aggregated = aggregated,
      covariate_type = covariateType
    )
  }

  # Cleanup
  sql <- "
    TRUNCATE TABLE #covariate_groups;
    DROP TABLE #covariate_groups;
  "
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql
  )

  # Return results
  results <- Andromeda::andromeda()
  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate("SELECT * FROM #pre_computed_covariate_table", targetDialect = attr(connection, "dbms")),
    andromeda = results,
    andromedaTableName = "covariates",
    snakeCaseToCamelCase = TRUE
  )

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate("SELECT * FROM #pre_computed_covariate_ref", targetDialect = attr(connection, "dbms")),
    andromeda = results,
    andromedaTableName = "covariateRef",
    snakeCaseToCamelCase = TRUE
  )

  results$analysisRef <- covariateGroups |>
    dplyr::cross_join(
      tibble::tibble(
        covariateType = c("Binary", "Counts", "AgeFirstEvent", "DaysToFirstEvent"),
        typeId = c(150, 250, 350, 450),
        isBinary = c(TRUE, FALSE, FALSE, FALSE),
        missingMeansZero = c(FALSE, TRUE, TRUE, TRUE)
      ) |>
        dplyr::filter(covariateType %in% covariateTypes)
    ) |>
    dplyr::transmute(
      analysisId = analysisGroupId + typeId,
      analysisName = paste0(covariateType, "|", analysisGroup),
      domainId = dplyr::if_else(analysisGroup %in% c("Condition", "Procedure", "Observation", "Device", "Measurements"), analysisGroup, "Source"),
      isBinary = isBinary,
      missingMeansZero = missingMeansZero
    )

  results$covariateRef <- results$covariateRef |>
    dplyr::left_join(results$analysisRef |> dplyr::select(analysisId, analysisName), by = "analysisId") |>
    dplyr::mutate(
      covariateName = paste0(analysisName, "|", covariateName)
    ) |>
    dplyr::select(-analysisName)

  # Construct analysis reference:
  metaData <- list(sql = sql, call = match.call())

  attr(results, "metaData") <- metaData
  class(results) <- "CovariateData"

  

  return(results)
}
