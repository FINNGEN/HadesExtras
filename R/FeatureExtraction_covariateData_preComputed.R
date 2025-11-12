
#' preComputed
#'
#' @description Executes pre-computed covariate extraction for a given cohort and covariate settings.
#'
#' @param connection A database connection object created using \code{DatabaseConnector::connect}.
#' @param tempEmulationSchema The temp schema where the covariate tables will be created.
#' @param cdmDatabaseSchema The schema where the CDM tables are located.
#' @param cdmVersion The version of the CDM.
#' @param cohortTable The table where the cohort data is located.
#' @param cohortIds The cohort IDs to include.
#' @param rowIdField The field in the cohort table that is the row ID.
#' @param covariateSettings A list of settings for the covariate data.
#' @param aggregated Logical. If TRUE, the covariate data is aggregated.
#' @param minCharacterizationMean The minimum mean for the covariate to be included.
#'
#' @importFrom DatabaseConnector querySql
#' @importFrom SqlRender render translate
#' @importFrom dplyr select mutate
#' @importFrom tidyr nest unnest
#' @importFrom purrr map
#' @importFrom tibble tibble
#' @importFrom Andromeda andromeda
#'
#' @export
#'
getPreComputedCovariates <- function(
  connection,
  cdmDatabaseSchema,
  resultsDatabaseSchema,
  personCodeCountsTable,
  covariateGroups,
  covariateTypes,
  cohortTableSchema,
  cohortTable = "#cohort_person",
  cohortIds = c(-1),
  aggregated = FALSE,
  minCharacterizationMean = 0
) {

  # VALIDATE
  connection |> checkmate::assertClass("DatabaseConnectorConnection")
  cdmDatabaseSchema |> checkmate::assertString()
  resultsDatabaseSchema |> checkmate::assertString()
  personCodeCountsTable |> checkmate::assertString()
  covariateGroups |> checkmate::assertDataFrame()
  covariateGroups |>
    names() |>
    checkmate::assertSetEqual(c("analysisGroup", "conceptClassId"))
  covariateTypes |> checkmate::assertSubset(c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous"))
  cohortTableSchema |> checkmate::assertString()
  cohortTable |> checkmate::assertString()
  cohortIds |> checkmate::assertNumeric()
  aggregated |> checkmate::assertLogical()
  minCharacterizationMean |> checkmate::assertNumber()


  # 1. Insert the covariate_groups table
  covariateGroups <- covariateGroups |>
    dplyr::group_by(analysisGroup) |>
    dplyr::mutate(analysisGroupId = dplyr::cur_group_id()) |>
    dplyr::ungroup()
  
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "covariate_groups",
    data = covariateGroups,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE,
    camelCaseToSnakeCase = TRUE
  )

  # All in one SQL file with conditional unions, makes it faster than with INSERTs
  sqlPath <- system.file("sql", "sql_server", "CovariatePreComputed.sql", package = "HadesExtras")
  sql <- SqlRender::readSql(sqlPath)
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    cohort_table_schema = cohortTableSchema,
    cohort_table_name = cohortTable,
    cohort_definition_id = paste0(cohortIds, collapse = ","),
    results_database_schema = resultsDatabaseSchema,
    cdm_database_schema = cdmDatabaseSchema,
    person_code_counts_table = personCodeCountsTable,
    include_binary = "Binary" %in% covariateTypes,
    include_categorical = "Categorical" %in% covariateTypes,
    include_counts = "Counts" %in% covariateTypes,
    include_continuous = "Continuous" %in% covariateTypes,
    include_age_first_event = "AgeFirstEvent" %in% covariateTypes,
    include_days_to_first_event = "DaysToFirstEvent" %in% covariateTypes
  )


  # Return results
  results <- Andromeda::andromeda()
  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate("SELECT * FROM #pre_computed_covariates", targetDialect = attr(connection, "dbms")),
    andromeda = results,
    andromedaTableName = "covariates",
    snakeCaseToCamelCase = TRUE
  )

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate("SELECT * FROM #pre_computed_covariates_continuous", targetDialect = attr(connection, "dbms")),
    andromeda = results,
    andromedaTableName = "covariatesContinuous",
    snakeCaseToCamelCase = TRUE
  )

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate("SELECT * FROM #pre_computed_concept_ref", targetDialect = attr(connection, "dbms")),
    andromeda = results,
    andromedaTableName = "conceptRef",
    snakeCaseToCamelCase = TRUE
  )

  results$analysisRef <- covariateGroups |>
    dplyr::cross_join(
      tibble::tibble(
        covariateType = c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous"),
        typeId = c(150, 250, 350, 450, 550, 650)
      ) |>
        dplyr::filter(covariateType %in% covariateTypes)
    ) |>
    dplyr::transmute(
      analysisId = analysisGroupId + typeId,
      analysisType = covariateType,
      analysisGroupId = analysisGroupId,
      domainId = dplyr::if_else(analysisGroup %in% c("Condition", "Procedure", "Observation", "Device", "Measurements"), analysisGroup, "Source")
    )

  return(results)
}
