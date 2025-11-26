#' getPreComputedCovariates
#'
#' @description
#' Extracts pre-computed covariate data for the specified cohort(s) and covariate group/type settings.
#'
#' @param connection A database connection object created using \code{DatabaseConnector::connect}.
#' @param cdmDatabaseSchema Name of the schema containing the OMOP Common Data Model (CDM) tables.
#' @param cohortTableSchema Name of the schema containing the cohort table.
#' @param cohortTable Name of the cohort table.
#' @param cohortIds A vector of cohort definition IDs to include.
#' @param resultsDatabaseSchema Name of the schema containing intermediate/results tables.
#' @param personCodeCountsTable Name of the table containing code/person counts or aggregates.
#' @param covariateGroups A data frame specifying the groups (with columns 'analysisGroup' and 'conceptClassId') of covariates to extract.
#' @param covariateTypes Character vector specifying covariate types to include (e.g. "Binary", "Categorical", etc.).
#' @param minCharacterizationMean Numeric; minimum mean for a covariate to be included in the output.
#'
#' @importFrom DatabaseConnector querySql insertTable
#' @importFrom SqlRender render translate readSql
#' @importFrom dplyr select mutate group_by ungroup cur_group_id
#' @importFrom tidyr nest unnest
#' @importFrom purrr map
#' @importFrom tibble tibble
#' @importFrom Andromeda andromeda
#'
#' @export
getPreComputedCovariatesAggregated <- function(
  connection,
  cdmDatabaseSchema,
  cohortTableSchema,
  cohortTable = "#cohort_person",
  cohortIds = c(-1),
  resultsDatabaseSchema,
  personCodeCountsTable,
  covariateGroups,
  covariateTypes,
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
  sqlPath <- system.file("sql", "sql_server", "CovariatePreComputedAgregated.sql", package = "HadesExtras")
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
  covariatesAndromeda <- Andromeda::andromeda()
  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate("SELECT * FROM #pre_computed_covariates", targetDialect = attr(connection, "dbms")),
    andromeda = covariatesAndromeda,
    andromedaTableName = "covariates",
    snakeCaseToCamelCase = TRUE
  )

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate("SELECT * FROM #pre_computed_covariates_continuous", targetDialect = attr(connection, "dbms")),
    andromeda = covariatesAndromeda,
    andromedaTableName = "covariatesContinuous",
    snakeCaseToCamelCase = TRUE
  )

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate("SELECT * FROM #pre_computed_concept_ref", targetDialect = attr(connection, "dbms")),
    andromeda = covariatesAndromeda,
    andromedaTableName = "conceptRef",
    snakeCaseToCamelCase = TRUE
  )

  analysisIdsInDatabase <- union(
    covariatesAndromeda$covariates |> dplyr::select(analysisId) |> dplyr::distinct() |> dplyr::pull(analysisId),
    covariatesAndromeda$covariatesContinuous |> dplyr::select(analysisId) |> dplyr::distinct() |> dplyr::pull(analysisId)
  ) |> unique()

  covariatesAndromeda$analysisRef <- covariateGroups |>
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
      domainId = dplyr::if_else(
        analysisGroup %in% c("Condition", "Procedure", "Observation", "Device", "Measurements"),
        analysisGroup,
        paste0("Source:", analysisGroup)
      )
    ) |>
    dplyr::distinct() |>
    dplyr::filter(analysisId %in% analysisIdsInDatabase)


  cohortCounts <- CohortGenerator::getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortTableSchema,
    cohortTable = cohortTable,
    cohortIds = cohortIds
  ) |>
    dplyr::collect()

  covariatesAndromeda$cohortCounts <- cohortCounts

  return(covariatesAndromeda)
}
