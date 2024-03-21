

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

  return(connectionStatusLog$log)
}


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


cohortTableToCohortDefinitionSettings <- function(
    cohortDatabaseSchema,
    cohortTable = "cohort",
    cohortDefinitionTable,
    cohortDefinitionIds,
    cohortIdOffset = 0L
){

  #
  # Validate parameters
  #
  cohortDatabaseSchema |> checkmate::assertString()
  cohortTable |> checkmate::assertString()
  cohortDefinitionIds |> checkmate::assertNumeric()
  cohortIdOffset |> checkmate::assertNumeric()

  #
  # Function
  #
  cohortDefinitionTable  |>
    dplyr::filter(cohort_definition_id %in% cohortDefinitionIds) |>
    dplyr::transmute(
      cohortId = as.double(cohort_definition_id+cohortIdOffset),
      cohortName = cohort_definition_name,
      json =  paste0(
        "{\"cohortDefinitionId\":", cohort_definition_id,
        ",\"cohortDefinitionName\":\"", cohort_definition_name,
        "\",\"cohortDefinitionDescription\":\"", cohort_definition_description, "\"}"
      ),
      sql = purrr::pmap_chr(.l = list(cohortDatabaseSchema, cohortTable, cohortId), .f=.cohortTableToSql)
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






















