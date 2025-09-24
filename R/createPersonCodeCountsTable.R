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
    sql <- SqlRender::render(baseSql,
        resultsDatabaseSchema = resultsDatabaseSchema,
        personCodeCountsTable = personCodeCountsTable,
        personCodeAtomicCountsTable = personCodeAtomicCountsTable,
        cdmDatabaseSchema = cdmDatabaseSchema
    )
    sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
    DatabaseConnector::executeSql(connection, sql)

    
}


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
        domain_id VARCHAR(255),
        person_id INTEGER,
        concept_id INTEGER,
        maps_to_concept_id INTEGER,
        n_records INTEGER,
        first_date DATE,
        first_age INTEGER,
        aggregated_value FLOAT,
        aggregated_value_unit INTEGER,
        aggregated_category INTEGER
    )"
    sql <- SqlRender::render(sql,
        resultsDatabaseSchema = resultsDatabaseSchema,
        personCodeAtomicCountsTable = personCodeAtomicCountsTable
    )
    sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
    DatabaseConnector::executeSql(connection, sql)

    # - Append to code atomic counts table
    sqlPath <- system.file("sql", "sql_server", "AppendToPersonAtomicCodeCountsTable.sql", package = "HadesExtras")
    baseSql <- SqlRender::readSql(sqlPath)

    for (i in 1:nrow(domains)) {
        domain <- domains[i, ]
        message(sprintf("Processing domain: %s", domain$table_name))
        sql <- SqlRender::render(baseSql,
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

        sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
        DatabaseConnector::executeSql(connection, sql)
    }
}
