
#' Check Existence of Cohort Definition Tables
#'
#' Validates the existence of the specified cohort definition and cohort tables within a given database schema.
#' Requires either an active database connection or connection details to create one. This function is intended
#' to assist in ensuring that the necessary tables for cohort definition are present in the database.
#'
#' @param connectionDetails Details required to establish a database connection (optional).
#' @param connection An existing database connection (optional).
#' @param cohortDatabaseSchema The database schema where the cohort definition tables are expected to be found.
#' @param cohortDefinitionTable The name of the cohort definition table. Defaults to "cohort_definition".
#' @param cohortTable The name of the table containing cohort data. Defaults to "cohort".
#'
#' @return A tibble log indicating the success or error in checking the existence of tables.
#'
#' @importFrom DatabaseConnector connect disconnect existsTable
#' @importFrom checkmate assertString
#'
#' @export
checkCohortDefinitionTables <- function(
    connectionDetails = NULL,
    connection = NULL,
    cohortDatabaseSchema,
    cohortDefinitionTable = "cohort_definition",
    cohortTable = "cohort"
    ){

  #
  # Validate parameters
  #
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  cohortDatabaseSchema |> checkmate::assertString()
  cohortDefinitionTable |> checkmate::assertString()
  cohortTable |> checkmate::assertString()

  #
  # Function
  #
  connectionStatusLog <- LogTibble$new()

  existCohortDefinitionTable <- DatabaseConnector::existsTable(connection, cohortDatabaseSchema, cohortDefinitionTable)
  if(!existCohortDefinitionTable){
    connectionStatusLog$ERROR(
      step = "check cohortDefinitionTable",
      message = "Cohort definition table does not exist."
    )
  }else{
    connectionStatusLog$SUCCESS(
      step = "check cohortDefinitionTable",
      message = "Cohort definition table exists."
    )
  }

  existCohortTable <- DatabaseConnector::existsTable(connection, cohortDatabaseSchema, cohortTable)
  if(!existCohortTable){
    connectionStatusLog$ERROR(
      step = "check cohortTable",
      message = "Cohort table does not exist."
    )
  }else{
    connectionStatusLog$SUCCESS(
      step = "check cohortTable",
      message = "Cohort table exists."
    )
  }

  return(connectionStatusLog)
}

#' Get Cohort Names from Cohort Definition Table
#'
#' Retrieves cohort names along with their corresponding IDs and descriptions from the specified cohort definition table
#' within a given database schema. This function assists in querying cohort information from the database.
#'
#' @param connectionDetails Details required to establish a database connection (optional).
#' @param connection An existing database connection (optional).
#' @param cohortDatabaseSchema The database schema where the cohort definition table is expected to be found.
#' @param cohortDefinitionTable The name of the cohort definition table. Defaults to "cohort_definition".
#'
#' @return A tibble containing cohort names, IDs, and descriptions.
#'
#' @importFrom DatabaseConnector connect disconnect dbGetQuery
#' @importFrom SqlRender render translate
#' @importFrom tibble as_tibble
#' @importFrom checkmate assertString
#'
#' @export
getCohortNamesFromCohortDefinitionTable <- function(
    connectionDetails = NULL,
    connection = NULL,
    cohortDatabaseSchema,
    cohortDefinitionTable = "cohort_definition"){

  #
  # Validate parameters
  #
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  cohortDatabaseSchema |> checkmate::assertString()
  cohortDefinitionTable |> checkmate::assertString()


  #
  # Function
  #
  sql  <- "SELECT cohort_definition_id, cohort_definition_name, cohort_definition_description FROM @cohort_database_schema.@cohort_definition_table"
  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_definition_table = cohortDefinitionTable,
    warnOnMissingParameters = TRUE
  )
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms
  )
  cohortDefinitionTable <- DatabaseConnector::dbGetQuery(connection, sql, progressBar = FALSE, reportOverallTime = FALSE) |>
    tibble::as_tibble()

  return(cohortDefinitionTable)

}

#' Convert Cohort Table to Cohort Definition Settings
#'
#' Converts cohort table entries into cohort definition settings for specified cohort IDs within a given database schema.
#' This function is useful for generating cohort definition settings from existing cohort data.
#'
#' @param cohortDatabaseSchema The database schema where the cohort table is located.
#' @param cohortTable The name of the cohort table. Defaults to "cohort".
#' @param cohortDefinitionTable The name of the cohort definition table where settings will be stored.
#' @param cohortDefinitionIds Numeric vector specifying cohort IDs to generate settings for.
#' @param cohortIdOffset Numeric value to add to cohort IDs. Defaults to 0.
#'
#' @return A tibble containing cohort definition settings.
#'
#' @importFrom dplyr filter transmute
#' @importFrom purrr pmap_chr
#' @importFrom checkmate assertString assertNumeric
#'
#' @export
cohortTableToCohortDefinitionSettings <- function(
    cohortDatabaseSchema,
    cohortTable = "cohort",
    cohortDefinitionTable,
    cohortDefinitionIds,
    newCohortDefinitionIds = cohortDefinitionIds
){

  #
  # Validate parameters
  #
  cohortDatabaseSchema |> checkmate::assertString()
  cohortTable |> checkmate::assertString()
  cohortDefinitionIds |> checkmate::assertNumeric()
  newCohortDefinitionIds |> checkmate::assertNumeric(len = length(cohortDefinitionIds))

  #
  # Function
  #
  cohortDefinitionTable  |>
    dplyr::filter(cohort_definition_id %in% cohortDefinitionIds) |>
    dplyr::transmute(
      cohortId = as.double(cohort_definition_id),
      cohortName = cohort_definition_name,
      json =  paste0(
        "{\"cohortDefinitionId\":", cohort_definition_id,
        ",\"cohortDefinitionName\":\"", cohort_definition_name,
        "\",\"cohortDefinitionDescription\":\"", cohort_definition_description, "\"}"
      ),
      sql = purrr::pmap_chr(.l = list(cohortDatabaseSchema, cohortTable, cohortId), .f=.cohortTableToSql)
    )  |>
    dplyr::mutate(
      cohortId = newCohortDefinitionIds
    )

}


.cohortTableToSql <- function(cohortDatabaseSchema, cohortTable, cohortDefinitionId){

  sql  <- c(
    "-- This code inserts records from an external cohort table into the cohort table of the cohort database schema.",
    "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;",
    "INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)",
    "SELECT @target_cohort_id AS cohort_definition_id, subject_id, cohort_start_date, cohort_end_date",
    "FROM  @source_database_schema.@source_cohort_table",
    "WHERE cohort_definition_id = @source_cohort_id;"
  ) |> paste(collapse = "\n")

  sql <- SqlRender::render(
    sql = sql,
    source_database_schema = cohortDatabaseSchema,
    source_cohort_table = cohortTable,
    source_cohort_id = cohortDefinitionId,
    warnOnMissingParameters = FALSE
  )

  return(sql)
}






















