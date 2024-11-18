#' Wrap around CohortGenerator::generateCohortSet
#'
#' @description
#' Wrap around CohortGenerator::generateCohortSet to be able to build cohorts based on cohortData.
#' Checks if cohortDefinitionSet contais a cohort based on cohortData.
#' If so, uploads cohortData found in the json column to the database and transforms that data in to a temporal table with cohortTable format.
#' CohortGenerator::generateCohortSet is called and processes the sql column that will copy the contents of the temporal cohortTable to the
#' target cohortTable.
#' It adds a column to the results tibble, 'buildInfo' with a LogTibble with information on the cohort construction.
#'
#' @param connectionDetails as in CohortGenerator::generateCohortSet.
#' @param connection as in CohortGenerator::generateCohortSet.
#' @param cdmDatabaseSchema as in CohortGenerator::generateCohortSet.
#' @param tempEmulationSchema as in CohortGenerator::generateCohortSet.
#' @param cohortDatabaseSchema as in CohortGenerator::generateCohortSet.
#' @param cohortTableNames as in CohortGenerator::generateCohortSet.
#' @param cohortDefinitionSet as in CohortGenerator::generateCohortSet.
#' @param stopOnError as in CohortGenerator::generateCohortSet.
#' @param incremental as in CohortGenerator::generateCohortSet.
#' @param incrementalFolder as in CohortGenerator::generateCohortSet.
#'
#' @returns results from CohortGenerator::generateCohortSet with additional column 'buildInfo'
#'
#' @importFrom checkmate assertDataFrame assertNames
#' @importFrom DatabaseConnector connect disconnect dbExistsTable dbRemoveTable dropEmulatedTempTables executeSql
#' @importFrom SqlRender render translate
#' @importFrom dplyr mutate filter left_join if_else select arrange bind_rows
#' @importFrom tidyr nest
#' @importFrom purrr map map2
#' @importFrom lubridate as_datetime
#'
#' @export

CohortGenerator_generateCohortSet <- function(
    connectionDetails = NULL,
    connection = NULL,
    cdmDatabaseSchema,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    cohortDatabaseSchema = cdmDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(),
    cohortDefinitionSet = NULL,
    stopOnError = TRUE,
    incremental = FALSE,
    incrementalFolder = NULL) {
  #
  # Validate parameters
  #
  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assertNames(colnames(cohortDefinitionSet),
    must.include = c(
      "cohortId",
      "cohortName",
      "sql"
    )
  )

  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  #
  # start function before generateCohortSet
  #

  # get cohortType from json
  cohortDefinitionSet <- cohortDefinitionSet |>
    dplyr::mutate(cohortType = purrr::map_chr(.x = json, .f = ~ {
      if (RJSONIO::isValidJSON(.x, asText = TRUE)) {
        l <- RJSONIO::fromJSON(.x)
        if ("cohortType" %in% names(l)) {
          return(l[["cohortType"]])
        }
      }
      return(as.character(NA))
    }))

  cohortDefinitionSetCohortDataType <- cohortDefinitionSet |>
    dplyr::filter(cohortType == "FromCohortData")

  # if incremental mode, ignore cohorts that are not changed
  if (incremental == TRUE) {

    cohortDefinitionSetCohortDataType <- cohortDefinitionSetCohortDataType |>
      dplyr::mutate(currentChecksum = CohortGenerator::computeChecksum(sql))

    recordKeepingFile <- file.path(incrementalFolder, "GeneratedCohorts.csv")
    if (file.exists(recordKeepingFile)) {       
      recordKeeping <- readr::read_csv(recordKeepingFile, show_col_types = FALSE) |>
        dplyr::rename(previousChecksum = checksum)

      cohortDefinitionSetCohortDataType <- cohortDefinitionSetCohortDataType |>
        dplyr::left_join(recordKeeping, by = c("cohortId" = "cohortId")) |>
        dplyr::filter(is.na(previousChecksum) | currentChecksum != previousChecksum) |>
        dplyr::select(cohortId, cohortName, json, sql, cohortType, currentChecksum)
    } 
  }

  if (nrow(cohortDefinitionSetCohortDataType) != 0) {
    # separate sql from cohortData
    cohortData <- .jsonToCohortData(cohortDefinitionSetCohortDataType)

    # Connect to tables and copy cohortData to database
    personTbl <- dplyr::tbl(connection, dbplyr::in_schema(cdmDatabaseSchema, "person"))
    observationPeriodTable <- dplyr::tbl(connection, dbplyr::in_schema(cdmDatabaseSchema, "observation_period"))
    cohortDataTable <- dplyr::copy_to(connection, cohortData, overwrite = TRUE, temporary = TRUE)

    # join to cohort_data_table cohort_names_table.cohort_name; person.person_id; observation_period period dates
    toAppend <- cohortDataTable |>
      dplyr::left_join(
        personTbl |>
          dplyr::select(person_id, person_source_value) |>
          dplyr::left_join(
            observationPeriodTable |>
              dplyr::select(person_id, observation_period_start_date, observation_period_end_date) |>
              dplyr::group_by(person_id) |>
              dplyr::summarise(
                observation_period_start_date = min(observation_period_start_date, na.rm = TRUE),
                observation_period_end_date = max(observation_period_end_date, na.rm = TRUE)
              ),
            by = c("person_id" = "person_id")
          ),
        by = c("person_source_value" = "person_source_value")
      )

    # collect check cohortData to add
    checkOnCohortData <- toAppend |>
      dplyr::group_by(cohort_definition_id) |>
      dplyr::summarise(
        n_source_person = dplyr::n_distinct(person_source_value),
        n_source_entries = dplyr::n(),
        n_missing_source_person = dplyr::n_distinct(person_source_value) - dplyr::n_distinct(person_id),
        n_missing_cohort_start = sum(ifelse(is.null(cohort_start_date), 1L, 0L), na.rm = TRUE),
        n_missing_cohort_end = sum(ifelse(is.null(cohort_end_date), 1L, 0L), na.rm = TRUE)
      ) |>
      dplyr::collect()
    
    # compute changes in toAppend
    toAppend <- toAppend |>
      dplyr::filter(!is.na(person_id)) |>
      # if cohort_start_date and cohort_end_date are na, use observation_period_start_date and observation_period_end_date
      dplyr::mutate(
        cohort_start_date = dplyr::if_else(is.na(cohort_start_date), observation_period_start_date, cohort_start_date),
        cohort_end_date = dplyr::if_else(is.na(cohort_end_date), observation_period_end_date, cohort_end_date)
      ) |>
      dplyr::select(cohort_definition_id, subject_id = person_id, cohort_start_date, cohort_end_date) |>
      dplyr::compute()

    # Update @source_cohort_table in the sql
    cohortDataImportTmpTableName <- toAppend |>
      dbplyr::remote_name()

    cohortDefinitionSetCohortDataType <- cohortDefinitionSetCohortDataType |>
      dplyr::mutate(sql = stringr::str_replace(sql, "@source_cohort_table", cohortDataImportTmpTableName))

    # replace recalculated cohortDefinitionSets
    cohortDefinitionSet <- dplyr::bind_rows(
      cohortDefinitionSet |> dplyr::filter(!(cohortId %in% cohortDefinitionSetCohortDataType$cohortId)),
      cohortDefinitionSetCohortDataType |> dplyr::select(cohortId, cohortName, json, sql, cohortType)
    ) |>
      dplyr::arrange(cohortId)
  }

  #
  # end function before generateCohortSet
  #

  results <- CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    stopOnError = stopOnError,
    incremental = incremental,
    incrementalFolder = incrementalFolder
  )


  cohortGeneratorResults <- results |>
    dplyr::select(-cohortName) |>
    dplyr::mutate(
      startTime = lubridate::as_datetime(startTime),
      endTime = lubridate::as_datetime(endTime),
      buildInfo = map(.x = cohortId, .f = ~ {
        LogTibble$new()
      })
    )

  #
  # start function after generateCohortSet
  #
  if (nrow(cohortDefinitionSetCohortDataType) != 0) {
    #
    cohortGeneratorResults <- cohortGeneratorResults |>
      dplyr::left_join(
        checkOnCohortData |> tidyr::nest(.key = "cohortDataInfo", .by = "cohort_definition_id"),
        by = c("cohortId" = "cohort_definition_id")
      ) |>
      dplyr::mutate(
        buildInfo = purrr::map2(.x = buildInfo, .y = cohortDataInfo, .f = .cohortDataInfoToBuildInfo)
      ) |>
      dplyr::select(-cohortDataInfo)

    DatabaseConnector::dropEmulatedTempTables(connection)

    # correct recordKeepingFile
    if (incremental == TRUE) {
      recordKeepingFile <- file.path(incrementalFolder, "GeneratedCohorts.csv")
      recordKeeping <- readr::read_csv(recordKeepingFile, show_col_types = FALSE) 

      recordKeeping <- recordKeeping |>
        dplyr::left_join(
          cohortDefinitionSetCohortDataType |> dplyr::select(cohortId, currentChecksum),
          by = c("cohortId" = "cohortId")
        ) |>
        dplyr::mutate(checksum = ifelse(is.na(currentChecksum), checksum, currentChecksum)) |>
        dplyr::select(-currentChecksum)

      readr::write_csv(recordKeeping, recordKeepingFile)
    }
  }

  #
  # end function after generateCohortSet
  #

  return(cohortGeneratorResults |> tibble::as_tibble())
}


.cohortDataInfoToBuildInfo <- function(buildInfo, cohortDataInfo) {
  buildInfo |> checkmate::assertR6(classes = "LogTibble")

  if (is.null(cohortDataInfo)) {
    return(buildInfo)
  }

  cohortDataInfo |> checkmate::assertTibble(nrows = 1)
  cohortDataInfo |>
    names() |>
    checkmate::assertNames(must.include = c("n_source_person", "n_source_entries", "n_missing_source_person", "n_missing_cohort_start", "n_missing_cohort_end"))


  if (cohortDataInfo$n_missing_source_person == 0) {
    buildInfo$SUCCESS("", "All person_source_values were found")
  }
  if (cohortDataInfo$n_missing_source_person == cohortDataInfo$n_source_person) {
    buildInfo$ERROR("", "None person_source_values were found")
  }
  if (cohortDataInfo$n_missing_source_person != 0 & cohortDataInfo$n_missing_source_person != cohortDataInfo$n_source_person) {
    buildInfo$WARNING("", cohortDataInfo$n_missing_source_person, "person_source_values were not found")
  }

  if (cohortDataInfo$n_missing_cohort_start != 0) {
    buildInfo$WARNING("", cohortDataInfo$n_missing_cohort_start, "cohort_start_dates were missing and set to the first observation date")
  }
  if (cohortDataInfo$n_missing_cohort_end != 0) {
    buildInfo$WARNING("", cohortDataInfo$n_missing_cohort_end, "cohort_end_dates were missing and set to the first observation date")
  }

  return(buildInfo)
}


#' deleteCohortFromCohortTable
#'
#' Deletes specified cohorts from a cohort table in a database.
#'
#' @param connectionDetails A list of database connection details (optional).
#' @param connection A pre-established database connection (optional).
#' @param cohortDatabaseSchema The schema name of the cohort database.
#' @param cohortTableNames A list containing the name of the cohort table.
#' @param cohortIds Numeric vector of cohort IDs to be deleted.
#' @param incrementalFolder The folder where the incremental file is stored (optional).
#'
#' @importFrom checkmate assertCharacter assertList assertNumeric
#' @importFrom DatabaseConnector connect disconnect executeSql
#' @importFrom SqlRender readSql render translate
#' @importFrom dplyr filter
#' @importFrom readr read_csv write_csv
#'
#' @return TRUE if the deletion was successful; otherwise, an error is raised.
#
#' @export
CohortGenerator_deleteCohortFromCohortTable <- function(
    connectionDetails = NULL,
    connection = NULL,
    cohortDatabaseSchema,
    cohortTableNames,
    cohortIds,
    incrementalFolder = NULL) {
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

  checkmate::assertCharacter(cohortDatabaseSchema, len = 1)
  checkmate::assertList(cohortTableNames)
  checkmate::assertNumeric(cohortIds)

  #
  # Function
  sql <- SqlRender::readSql(system.file("sql/sql_server/DeleteCohortFromCohortTables.sql", package = "HadesExtras", mustWork = TRUE))
  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTableNames$cohortTable,
    cohort_ids = paste0("(", paste0(cohortIds, collapse = " ,"), ")"),
    warnOnMissingParameters = TRUE
  )
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms
  )
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

  if (!is.null(incrementalFolder)) {
    recordKeepingFile <- file.path(incrementalFolder, "GeneratedCohorts.csv")
    generatedCohorts <- readr::read_csv(recordKeepingFile, show_col_types = FALSE)
    generatedCohorts <- generatedCohorts |>
      dplyr::filter(!cohortId %in% cohortIds)
    if (nrow(generatedCohorts) == 0) {
      unlink(recordKeepingFile)
    } else {
      readr::write_csv(generatedCohorts, recordKeepingFile)
    }
  }

  return(TRUE)
}


#' Get cohort counts and basic cohort demographics for a specific cohort.
#'
#' This function retrieves demographic information for a given cohort .
#'
#' @param connectionDetails Details required to establish a database connection (optional).
#' @param connection An existing database connection (optional).
#' @param cdmDatabaseSchema The schema name for the Common Data Model (CDM) database.
#' @param vocabularyDatabaseSchema The schema name for the vocabulary database (default is \code{cdmDatabaseSchema}).
#' @param cohortDatabaseSchema The schema name for the cohort database.
#' @param cohortTable The name of the cohort table in the cohort database (default is "cohort").
#' @param cohortIds A numeric vector of cohort IDs for which to retrieve demographics (default is an empty vector).
#' @param toGet A character vector indicating which demographic information to retrieve. Possible values include "histogramCohortStartYear", "histogramCohortEndYear", "histogramBirthYear", and "sexCounts".
#' @param cohortDefinitionSet A set of cohort definitions (optional).
#' @param databaseId The ID of the database (optional).
#'
#' @return
#' A data frame with cohort counts and selected demographics
#'
#' @importFrom DatabaseConnector connect disconnect getTableNames
#' @importFrom dplyr tbl count collect mutate left_join distinct select nest_by
#' @importFrom CohortGenerator getCohortCounts
#'
#' @export
CohortGenerator_getCohortDemograpics <- function(
    connectionDetails = NULL,
    connection = NULL,
    cdmDatabaseSchema,
    vocabularyDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable = "cohort",
    cohortIds = c(),
    toGet = c("histogramCohortStartYear", "histogramCohortEndYear", "histogramBirthYear", "sexCounts"),
    cohortDefinitionSet = NULL,
    databaseId = NULL) {
  start <- Sys.time()

  #
  # validate parameters
  #
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  tablesInServer <- tolower(DatabaseConnector::getTableNames(conn = connection, databaseSchema = cohortDatabaseSchema))
  if (!(tolower(cohortTable) %in% tablesInServer)) {
    warning("Cohort table was not found. Was it created?")
    return(NULL)
  }

  toGet |> checkmate::assertSubset(c("histogramCohortStartYear", "histogramCohortEndYear", "histogramBirthYear", "sexCounts"))

  #
  # function
  #
  cohortTbl <- dplyr::tbl(connection, dbplyr::in_schema(cohortDatabaseSchema, cohortTable))
  personTbl <- dplyr::tbl(connection, dbplyr::in_schema(cdmDatabaseSchema, "person"))
  conceptTbl <- dplyr::tbl(connection, dbplyr::in_schema(vocabularyDatabaseSchema, "concept"))

  cohortCounts <- CohortGenerator::getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohortIds,
    cohortDefinitionSet = cohortDefinitionSet,
    databaseId = databaseId
  ) |> tibble::as_tibble()

  histogramCohortStartYear <- tibble::tibble(cohort_definition_id = 0, .rows = 0)
  if ("histogramCohortStartYear" %in% toGet) {
    histogramCohortStartYear <- cohortTbl |>
      dplyr::mutate(year = year(cohort_start_date)) |>
      dplyr::count(cohort_definition_id, year) |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "histogramCohortStartYear")
  }

  histogramCohortEndYear <- tibble::tibble(cohort_definition_id = 0, .rows = 0)
  if ("histogramCohortEndYear" %in% toGet) {
    histogramCohortEndYear <- cohortTbl |>
      dplyr::mutate(year = year(cohort_end_date)) |>
      dplyr::count(cohort_definition_id, year) |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "histogramCohortEndYear")
  }

  histogramBirthYear <- tibble::tibble(cohort_definition_id = 0, .rows = 0)
  if ("histogramBirthYear" %in% toGet) {
    histogramBirthYear <- cohortTbl |>
      dplyr::distinct(cohort_definition_id, subject_id) |>
      dplyr::left_join(
        personTbl |> dplyr::select(person_id, year_of_birth),
        by = c("subject_id" = "person_id")
      ) |>
      dplyr::rename(year = year_of_birth) |>
      dplyr::count(cohort_definition_id, year) |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "histogramBirthYear")
  }

  sexCounts <- tibble::tibble(cohort_definition_id = 0, .rows = 0)
  if ("sexCounts" %in% toGet) {
    sexCounts <- cohortTbl |>
      dplyr::left_join(
        personTbl |> dplyr::select(person_id, gender_concept_id),
        by = c("subject_id" = "person_id")
      ) |>
      dplyr::left_join(
        conceptTbl |> dplyr::select(concept_id, concept_name),
        by = c("gender_concept_id" = "concept_id")
      ) |>
      dplyr::count(cohort_definition_id, sex = concept_name) |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "sexCounts")
  }

  cohortDemograpics <- cohortCounts |>
    dplyr::left_join(histogramCohortStartYear, by = c("cohortId" = "cohort_definition_id")) |>
    dplyr::left_join(histogramCohortEndYear, by = c("cohortId" = "cohort_definition_id")) |>
    dplyr::left_join(histogramBirthYear, by = c("cohortId" = "cohort_definition_id")) |>
    dplyr::left_join(sexCounts, by = c("cohortId" = "cohort_definition_id"))


  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("getCohortDemograpics took", signif(delta, 3), attr(delta, "units")))

  return(cohortDemograpics)
}


#' CohortGenerator_getCohortsOverlaps
#'
#' Description: A function to calculate overlaps between cohorts.
#'
#' @param connectionDetails A list containing details for connecting to the database. Default is NULL.
#' @param connection A database connection object. Default is NULL.
#' @param cohortDatabaseSchema The schema where the cohort table resides.
#' @param cohortTable The name of the cohort table.
#' @param cohortIds A vector of cohort ids for which to calculate overlaps. Default is an empty vector.
#'
#' @return A tibble containing the overlaps between cohorts.
#' Column `cohortIdCombinations` indicates the cohort ids separated by - in the combination `numberOfSubjects` with the number of subjects in the combination.
#'
#' @importFrom DatabaseConnector connect disconnect querySql
#' @importFrom checkmate assertCharacter assertString assertNumeric
#' @importFrom SqlRender readSql render translate
#' @importFrom stringr str_split
#' @importFrom tidyr separate_wider_delim
#' @importFrom tibble as_tibble
#' @importFrom dplyr pull
#'
#' @export
CohortGenerator_getCohortsOverlaps <- function(
    connectionDetails = NULL,
    connection = NULL,
    cohortDatabaseSchema,
    cohortTable = "cohort",
    cohortIds = c()) {
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

  checkmate::assertCharacter(cohortDatabaseSchema, len = 1)
  checkmate::assertString(cohortTable)
  checkmate::assertNumeric(cohortIds, null.ok = TRUE)

  #
  # Function
  sql <- SqlRender::readSql(system.file("sql/sql_server/CalculateCohortsOverlap.sql", package = "HadesExtras", mustWork = TRUE))
  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds,
    warnOnMissingParameters = TRUE
  )
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms
  )

  overlaps <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE) |>
    tibble::as_tibble() |>
    dplyr::mutate(cohortIdCombinations = paste0("-", cohortIdCombinations, "-"))

  return(overlaps)
}


#' Remove specified cohort IDs from cohort overlaps table
#'
#' This function removes specified cohort IDs from a cohort overlaps table.
#'
#' @param cohortOverlaps A data frame containing cohort overlaps.
#' @param cohortIds A numeric vector of cohort IDs to be removed.
#' @return A data frame with cohort overlaps after removing specified IDs.
#' @export
#' @importFrom dplyr mutate group_by summarize filter
#' @importFrom checkmate assertDataFrame assertSubset assertNumeric
#' @importFrom purrr map_chr
#' @importFrom stringr str_split str_detect
#'
#' @export
removeCohortIdsFromCohortOverlapsTable <- function(cohortOverlaps, cohortIds) {
  if (length(cohortIds) == 0) {
    return(cohortOverlaps)
  }

  cohortOverlaps |> checkmate::assertDataFrame()
  cohortOverlaps |>
    names() |>
    checkmate::assertSubset(c("cohortIdCombinations", "numberOfSubjects"))
  cohortIds |> checkmate::assertNumeric()

  cohortOverlaps <- cohortOverlaps |>
    dplyr::mutate(
      cohortIdCombinations = purrr::map_chr(cohortIdCombinations, ~ {
        a <- stringr::str_split(.x, "-")[[1]] |>
          setdiff(c("", as.character(cohortIds))) |>
          paste0(collapse = "-")
        a <- paste0("-", a, "-")
      }),
    ) |>
    dplyr::filter(!stringr::str_detect(cohortIdCombinations, "--")) |>
    dplyr::group_by(cohortIdCombinations) |>
    dplyr::summarize(numberOfSubjects = sum(numberOfSubjects), .groups = "drop")

  return(cohortOverlaps)
}


#' CohortGenerator_createCohortTables
#'
#' Wrapper for CohortGenerator::createCohortTables, where if bigquery is used, the tables are created using bigrquery::bq_table_create.
#' 
#' @param connectionDetails An object of class \code{connectionDetails} containing database connection details.
#' @param connection A database connection object created using \code{DatabaseConnector::connect}.
#' @param cohortDatabaseSchema The schema where the cohort tables will be created.
#' @param cohortTableNames A list containing the names of the cohort tables to be created.
#' @param incremental Logical. If TRUE, only creates tables that don't already exist.
#'
#' @importFrom bigrquery bq_table bq_table_exists bq_table_delete bq_table_create
#' @importFrom tibble tibble
#' @importFrom DatabaseConnector connect disconnect
#' @importFrom CohortGenerator createCohortTables getCohortTableNames
#' 
#' @export
CohortGenerator_createCohortTables <- function(
  connectionDetails = NULL,
  connection = NULL,
  cohortDatabaseSchema,
  cohortTableNames = CohortGenerator::getCohortTableNames(),
  incremental = FALSE) {

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (connection@dbms == "bigquery" && "dbiConnection" %in% slotNames(connection)) {

    strings <- strsplit(cohortDatabaseSchema, "\\.")
    bq_project <- strings[[1]][1]
    bq_dataset <- strings[[1]][2]

    # Create table templates based on SQL schema
    cohortTableTemplate <- tibble::tibble(
      cohort_definition_id = as.integer(),
      subject_id = as.integer(),
      cohort_start_date = as.Date(as.character()),
      cohort_end_date = as.Date(as.character())
    )

    inclusionTableTemplate <- tibble::tibble(
      cohort_definition_id = as.integer(),
      rule_sequence = as.integer(),
      name = as.character(),
      description = as.character()
    )

    inclusionResultTableTemplate <- tibble::tibble(
      cohort_definition_id = as.integer(),
      inclusion_rule_mask = as.integer(),
      person_count = as.integer(),
      mode_id = as.integer()
    )

    inclusionStatsTableTemplate <- tibble::tibble(
      cohort_definition_id = as.integer(),
      rule_sequence = as.integer(),
      person_count = as.integer(),
      gain_count = as.integer(),
      person_total = as.integer(),
      mode_id = as.integer()
    )

    summaryStatsTableTemplate <- tibble::tibble(
      cohort_definition_id = as.integer(),
      base_count = as.integer(),
      final_count = as.integer(),
      mode_id = as.integer()
    )

    censorStatsTableTemplate <- tibble::tibble(
      cohort_definition_id = as.integer(),
      lost_count = as.integer()
    )

    # Create or replace tables based on templates
    tableTemplates <- list(
      cohortTable = cohortTableTemplate,
      inclusionTable = inclusionTableTemplate,
      inclusionResultTable = inclusionResultTableTemplate,
      inclusionStatsTable = inclusionStatsTableTemplate,
      summaryStatsTable = summaryStatsTableTemplate,
      censorStatsTable = censorStatsTableTemplate
    )

    for (tableName in names(tableTemplates)) {
      tableSuffix <- switch(tableName,
        "cohortTable" = "",
        "inclusionTable" = "_inclusion",
        "inclusionResultTable" = "_inclusion_result",
        "inclusionStatsTable" = "_inclusion_stats",
        "summaryStatsTable" = "_summary_stats",
        "censorStatsTable" = "_censor_stats"
      )
      
      fullTableName <- paste0(cohortTableNames$cohortTable, tableSuffix)
      bq_table <- bigrquery::bq_table(bq_project, bq_dataset, fullTableName)
      
      if (bigrquery::bq_table_exists(bq_table) && !incremental) {
        message(paste("Replacing bigquery table", fullTableName))
        bigrquery::bq_table_delete(bq_table)
        bigrquery::bq_table_create(bq_table, fields = tableTemplates[[tableName]])
      }

      if (!bigrquery::bq_table_exists(bq_table)) {
        message(paste("Creating bigquery table", fullTableName))
        bigrquery::bq_table_create(bq_table, fields = tableTemplates[[tableName]])
      }
    }

  } else {
    CohortGenerator::createCohortTables(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      incremental = incremental
    )
  }
}

#' CohortGenerator_dropCohortStatsTables
#' 
#' Wrapper for CohortGenerator::dropCohortStatsTables, where if bigquery is used, the tables are deleted using bigrquery::bq_table_delete.
#' 
#' @param connectionDetails An object of class \code{connectionDetails} containing database connection details.
#' @param connection A database connection object created using \code{DatabaseConnector::connect}.
#' @param cohortDatabaseSchema The schema where the cohort tables will be created.
#' @param cohortTableNames A list containing the names of the cohort tables to be created.
#'
#' @importFrom bigrquery bq_table bq_table_exists bq_table_delete
#' @importFrom CohortGenerator dropCohortStatsTables getCohortTableNames
#'
#' @export
CohortGenerator_dropCohortStatsTables <- function(
  connectionDetails = NULL,
  connection = NULL,
  cohortDatabaseSchema,
  cohortTableNames = CohortGenerator::getCohortTableNames()) {

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (connection@dbms == "bigquery" && "dbiConnection" %in% slotNames(connection)) {
    strings <- strsplit(cohortDatabaseSchema, "\\.")
    bq_project <- strings[[1]][1]
    bq_dataset <- strings[[1]][2]

    # Define all table suffixes (including empty for main cohort table)
    tableSuffixes <- c(
      "",  # for main cohort table
      "_inclusion",
      "_inclusion_result",
      "_inclusion_stats",
      "_summary_stats",
      "_censor_stats"
    )

    # Delete all related tables
    for (suffix in tableSuffixes) {
      tableName <- paste0(cohortTableNames$cohortTable, suffix)
      bq_table <- bigrquery::bq_table(bq_project, bq_dataset, tableName)
      
      if (bigrquery::bq_table_exists(bq_table)) {
        message(paste("Deleting bigquery table", tableName))
        bigrquery::bq_table_delete(bq_table)
      }
    }
  } else {
    CohortGenerator::dropCohortStatsTables(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      dropCohortTable = TRUE
    )
  }
}
