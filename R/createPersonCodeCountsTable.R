#' Create Person Code Counts Table
#'
#' Creates a person code counts table by first creating an atomic person code counts table
#' and then aggregating the data into a final code counts table.
#'
#' @param CDMdbHandler A CDMdbHandler object containing database connection details
#' @param personCodeCountsTable Name of the person code counts table to create (default: "person_code_counts")
#'
#' @return No return value, creates tables in the database
#'
#' @importFrom checkmate assertClass assertString
#' @importFrom SqlRender readSql render translate
#' @importFrom DatabaseConnector executeSql
#'
#' @export
createPersonCodeCountsTable <- function(
    CDMdbHandler,
    personCodeCountsTable = "person_code_counts") {
    #
    # VALIDATE
    #
    CDMdbHandler |> checkmate::assertClass("CDMdbHandler")
    connection <- CDMdbHandler$connectionHandler$getConnection()
    vocabularyDatabaseSchema <- CDMdbHandler$vocabularyDatabaseSchema
    cdmDatabaseSchema <- CDMdbHandler$cdmDatabaseSchema
    resultsDatabaseSchema <- CDMdbHandler$resultsDatabaseSchema

    personCodeCountsTable |> checkmate::assertString()


    #
    # FUNCTION
    #

    # - Create atomic person code counts table
    personCodeAtomicCountsTable <- paste0("atomic_", personCodeCountsTable)

    createPersonCodeAtomicCountsTable(CDMdbHandler, personCodeAtomicCountsTable = personCodeAtomicCountsTable)
    
    # - Create code counts table
    sqlPath <- system.file("sql", "sql_server", "CreateCodeCountsTable.sql", package = "HadesExtras")
    baseSql <- SqlRender::readSql(sqlPath)
    DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = baseSql,
        resultsDatabaseSchema = resultsDatabaseSchema,
        personCodeCountsTable = personCodeCountsTable,
        personCodeAtomicCountsTable = personCodeAtomicCountsTable,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cdmDatabaseSchema = cdmDatabaseSchema
    )

    
}


#' Create Person Code Atomic Counts Table
#'
#' Creates an atomic person code counts table by processing different domains (Condition, Procedure, 
#' Measurement, Drug, Observation, Device) and aggregating code counts per person.
#'
#' @param CDMdbHandler A CDMdbHandler object containing database connection details
#' @param domains A data frame specifying domain configurations. If NULL, uses default domains.
#'   Must contain columns: domain_id, table_name, concept_id_field, start_date_field, 
#'   end_date_field, maps_to_concept_id_field
#' @param personCodeAtomicCountsTable Name of the atomic person code counts table to create 
#'   (default: "atomic_person_code_counts")
#'
#' @return No return value, creates tables in the database
#'
#' @importFrom checkmate assertClass assertDataFrame assertSetEqual
#' @importFrom tibble tribble
#' @importFrom SqlRender render translate
#' @importFrom DatabaseConnector executeSql
#'
#' @export
createPersonCodeAtomicCountsTable <- function(
    CDMdbHandler,
    domains = NULL,
    personCodeAtomicCountsTable = "atomic_person_code_counts") {
    #
    # VALIDATE
    #
    CDMdbHandler |> checkmate::assertClass("CDMdbHandler")
    connection <- CDMdbHandler$connectionHandler$getConnection()
    vocabularyDatabaseSchema <- CDMdbHandler$vocabularyDatabaseSchema
    cdmDatabaseSchema <- CDMdbHandler$cdmDatabaseSchema
    resultsDatabaseSchema <- CDMdbHandler$resultsDatabaseSchema

    if (is.null(domains)) {
        domains <- tibble::tribble(
            ~domain_id, ~table_name, ~concept_id_field, ~start_date_field, ~end_date_field, ~maps_to_concept_id_field,
            # non standard
            "Condition", "condition_occurrence", "condition_concept_id", "condition_start_date", "condition_end_date", "condition_source_concept_id",
            "Procedure", "procedure_occurrence", "procedure_concept_id", "procedure_date", "procedure_date", "procedure_source_concept_id",
            "Measurement", "measurement", "measurement_concept_id", "measurement_date", "measurement_date", "measurement_concept_id",
            "Drug", "drug_exposure", "drug_concept_id", "drug_exposure_start_date", "drug_exposure_end_date", "drug_concept_id",
            "Observation", "observation", "observation_concept_id", "observation_date", "observation_date", "observation_source_concept_id",
            "Device", "device_exposure", "device_concept_id", "device_exposure_start_date", "device_exposure_end_date", "device_source_concept_id"
        )
    }

    domains |> checkmate::assertDataFrame()
    domains |>
        names() |>
        checkmate::assertSetEqual(c("domain_id", "table_name", "concept_id_field", "start_date_field", "end_date_field", "maps_to_concept_id_field"))

    #
    # FUNCTION
    #

    # - Create code atomic counts table
    sql <- "DROP TABLE IF EXISTS @resultsDatabaseSchema.@personCodeAtomicCountsTable;
    CREATE TABLE @resultsDatabaseSchema.@personCodeAtomicCountsTable (
        domain_id VARCHAR(255) NOT NULL,
        person_id BIGINT NOT NULL,
        concept_id BIGINT,
        maps_to_concept_id BIGINT,
        n_records INT,
        first_date DATE,
        first_age INT,
        aggregated_value FLOAT,
        aggregated_value_unit INT,
        aggregated_category INT
    )"
    DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        resultsDatabaseSchema = resultsDatabaseSchema,
        personCodeAtomicCountsTable = personCodeAtomicCountsTable
    )

    # - Append to code atomic counts table
    sqlPath <- system.file("sql", "sql_server", "AppendToPersonAtomicCodeCountsTable.sql", package = "HadesExtras")
    baseSql <- SqlRender::readSql(sqlPath)

    for (i in 1:nrow(domains)) {
        domain <- domains[i, ]
        message(sprintf("Processing domain: %s", domain$table_name))
        DatabaseConnector::renderTranslateExecuteSql(
            connection = connection,
            sql = baseSql,
            domain_id = domain$domain_id,
            personCodeAtomicCountsTable = personCodeAtomicCountsTable,
            cdmDatabaseSchema = cdmDatabaseSchema,
            resultsDatabaseSchema = resultsDatabaseSchema,
            table_name = domain$table_name,
            concept_id_field = domain$concept_id_field,
            start_date_field = domain$start_date_field,
            end_date_field = domain$end_date_field,
            maps_to_concept_id_field = domain$maps_to_concept_id_field
        )
    }
}
