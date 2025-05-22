#' CDM Database Handler Class
#'
#' @description
#' A class for handling CDM database connections and operations
#'
#' @export
#' @importFrom R6 R6Class
#'
#' @field databaseId The ID of the database
#' @field databaseName The name of the database
#' @field databaseDescription Description of the database
#' @field connectionHandler Handler for database connections
#' @field vocabularyDatabaseSchema Schema name for the vocabulary database
#' @field cdmDatabaseSchema Schema name for the CDM database
#' @field connectionStatusLog Log of connection status and operations
#' @field vocabularyInfo Information about the vocabulary tables
#' @field CDMInfo Information about the CDM structure
#' @field getTblVocabularySchema Function to get vocabulary schema table
#' @field getTblCDMSchema Function to get CDM schema table
#'
#' @section Methods:
#' \describe{
#'   \item{`initialize(databaseId)`}{Initialize a new CDM database handler}
#'   \item{`loadConnection(loadConnectionChecksLevel)`}{Load database connection with specified check level}
#' }
#'
#' @param databaseId ID of the database to connect to
#' @param loadConnectionChecksLevel Level of connection checks to perform
CDMdbHandler <- R6::R6Class(
  classname = "CDMdbHandler",
  private = list(
    .databaseId = NULL,
    .databaseName = NULL,
    .databaseDescription = NULL,
    # database parameters
    .connectionHandler = NULL,
    .vocabularyDatabaseSchema = NULL,
    .cdmDatabaseSchema = NULL,
    .resultsDatabaseSchema = NULL,
    .connectionStatusLog = NULL,
    #
    .vocabularyInfo = NULL,
    .CDMInfo = NULL,
    #
    .getTblVocabularySchema = NULL,
    .getTblCDMSchema = NULL
  ),
  active = list(
    databaseId = function() {
      return(private$.databaseId)
    },
    databaseName = function() {
      return(private$.databaseName)
    },
    databaseDescription = function() {
      return(private$.databaseDescription)
    },
    # database parameters
    connectionHandler = function() {
      return(private$.connectionHandler)
    },
    vocabularyDatabaseSchema = function() {
      return(private$.vocabularyDatabaseSchema)
    },
    cdmDatabaseSchema = function() {
      return(private$.cdmDatabaseSchema)
    },
    resultsDatabaseSchema = function() {
      return(private$.resultsDatabaseSchema)
    },
    connectionStatusLog = function() {
      return(private$.connectionStatusLog$logTibble |>
        dplyr::mutate(databaseId = private$.databaseId, databaseName = private$.databaseName) |>
        dplyr::relocate(databaseId, databaseName, .before = 1))
    },
    #
    vocabularyInfo = function() {
      return(private$.vocabularyInfo)
    },
    CDMInfo = function() {
      return(private$.CDMInfo)
    },
    #
    getTblVocabularySchema = function() {
      return(private$.getTblVocabularySchema)
    },
    getTblCDMSchema = function() {
      return(private$.getTblCDMSchema)
    }
  ),
  public = list(
    #'
    #' @param databaseId             A text id for the database the it connects to
    #' @param databaseName           A text name for the database the it connects to
    #' @param databaseDescription    A text description for the database the it connects to
    #' @param connectionHandler             A ConnectionHandler object
    #' @param cdmDatabaseSchema             Name of the CDM database schema
    #' @param vocabularyDatabaseSchema      (Optional) Name of the vocabulary database schema (default is cdmDatabaseSchema)
    #' @param loadConnectionChecksLevel     (Optional) Level of checks to perform when loading the connection (default is "allChecks")
    initialize = function(databaseId,
                          databaseName,
                          databaseDescription,
                          connectionHandler,
                          cdmDatabaseSchema,
                          vocabularyDatabaseSchema = cdmDatabaseSchema,
                          resultsDatabaseSchema = cdmDatabaseSchema,
                          loadConnectionChecksLevel = "allChecks") {
      checkmate::assertString(databaseId)
      checkmate::assertString(databaseName, )
      checkmate::assertString(databaseDescription)
      checkmate::assertClass(connectionHandler, "ConnectionHandler")
      checkmate::assertString(cdmDatabaseSchema)
      checkmate::assertString(vocabularyDatabaseSchema)
      checkmate::assertString(resultsDatabaseSchema)
      private$.databaseId <- databaseId
      private$.databaseName <- databaseName
      private$.databaseDescription <- databaseDescription
      private$.connectionHandler <- connectionHandler
      private$.vocabularyDatabaseSchema <- vocabularyDatabaseSchema
      private$.cdmDatabaseSchema <- cdmDatabaseSchema
      private$.resultsDatabaseSchema <- resultsDatabaseSchema

      self$loadConnection(loadConnectionChecksLevel)
    },

    #' Finalize method
    #' @description
    #' Closes the connection if active.
    finalize = function() {
      private$.connectionHandler$finalize()
    },

    #' Reload connection
    #' @description
    #' Updates the connection status by checking the database connection, vocabulary database schema, and CDM database schema.
    loadConnection = function(loadConnectionChecksLevel) {
      checkmate::assertString(loadConnectionChecksLevel)
      checkmate::assertSubset(loadConnectionChecksLevel, c("dontConnect", "basicChecks", "allChecks"))

      connectionStatusLog <- LogTibble$new()

      if (loadConnectionChecksLevel == "dontConnect") {
        private$.connectionStatusLog <- connectionStatusLog
        return()
      }

      # Check db connection
      errorMessage <- ""
      tryCatch(
        {
          private$.connectionHandler$initConnection()
        },
        error = function(error) {
          errorMessage <<- error$message
        },
        warning = function(warning) {}
      )

      if (errorMessage != "" | !private$.connectionHandler$dbIsValid()) {
        connectionStatusLog$ERROR("Check database connection", errorMessage)
      } else {
        connectionStatusLog$SUCCESS("Check database connection", "Valid connection")
      }

      # Check can create temp tables
      if (loadConnectionChecksLevel == "allChecks") {
        errorMessage <- ""
        tryCatch(
          {
            private$.connectionHandler$getConnection() |>
              DatabaseConnector::insertTable(
                tableName = "test_temp_table",
                data = dplyr::tibble(x = 1),
                tempTable = TRUE
              )
            private$.connectionHandler$getConnection() |>
              DatabaseConnector::dropEmulatedTempTables()
          },
          error = function(error) {
            errorMessage <<- error$message
          }
        )

        if (errorMessage != "") {
          connectionStatusLog$ERROR("Check temp table creation", errorMessage)
        } else {
          connectionStatusLog$SUCCESS("Check temp table creation", "can create temp tables")
        }
      } else {
        connectionStatusLog$WARNING("Check temp table creation", "skipped")
      }

      # Check vocabularyDatabaseSchema and populates getTblVocabularySchema
      getTblVocabularySchema <- list()
      vocabularyInfo <- NULL
      errorMessage <- ""
      tryCatch(
        {
          # create dbplyr object for all tables in the vocabulary
          vocabularyTableNames <- c(
            "concept",
            "vocabulary",
            "domain",
            "concept_class",
            "concept_relationship",
            "relationship",
            "concept_synonym",
            "concept_ancestor",
            "source_to_concept_map",
            "drug_strength"
          )

          tablesInVocabularyDatabaseSchema <- DatabaseConnector::getTableNames(private$.connectionHandler$getConnection(), private$.vocabularyDatabaseSchema)

          vocabularyTablesInVocabularyDatabaseSchema <- intersect(vocabularyTableNames, tablesInVocabularyDatabaseSchema)

          for (tableName in vocabularyTablesInVocabularyDatabaseSchema) {
            text <- paste0('function() { private$.connectionHandler$tbl( "', tableName, '", "', private$.vocabularyDatabaseSchema, '")}')
            getTblVocabularySchema[[tableName]] <- eval(parse(text = text))
          }

          vocabularyInfo <- getTblVocabularySchema$vocabulary() |>
            dplyr::filter(vocabulary_id == "None") |>
            dplyr::select(vocabulary_name, vocabulary_version) |>
            dplyr::collect(n = 1)
        },
        error = function(error) {
          errorMessage <<- error$message
        }
      )

      if (errorMessage != "") {
        connectionStatusLog$ERROR("vocabularyDatabaseSchema connection", errorMessage)
      } else {
        connectionStatusLog$SUCCESS(
          "vocabularyDatabaseSchema connection",
          "Connected to vocabulary:", vocabularyInfo$vocabulary_name,
          "Version: ", vocabularyInfo$vocabulary_version
        )
      }


      # Check cdmDatabaseSchema and populates getTblCDMSchema
      getTblCDMSchema <- list()
      CDMInfo <- NULL
      errorMessage <- ""
      tryCatch(
        {
          cdmTableNames <- c(
            "person",
            "observation_period",
            "visit_occurrence",
            "visit_detail",
            "condition_occurrence",
            "drug_exposure",
            "procedure_occurrence",
            "device_exposure",
            "measurement",
            "observation",
            "death",
            "note",
            "note_nlp",
            "specimen",
            "fact_relationship",
            "location",
            "care_site",
            "provider",
            "payer_plan_period",
            "cost",
            "drug_era",
            "dose_era",
            "condition_era",
            "episode",
            "episode_event",
            "metadata",
            "cdm_source"
          )

          tablesInCdmDatabaseSchema <- DatabaseConnector::getTableNames(private$.connectionHandler$getConnection(), private$.cdmDatabaseSchema)

          vocabularyTablesInCdmDatabaseSchema <- intersect(cdmTableNames, tablesInCdmDatabaseSchema)

          for (tableName in vocabularyTablesInCdmDatabaseSchema) {
            text <- paste0('function() { private$.connectionHandler$tbl( "', tableName, '", "', private$.cdmDatabaseSchema, '")}')
            getTblCDMSchema[[tableName]] <- eval(parse(text = text))
          }

          CDMInfo <- getTblCDMSchema$cdm_source() |>
            dplyr::select(cdm_source_name, cdm_source_abbreviation, cdm_version) |>
            dplyr::collect(n = 1)
        },
        error = function(error) {
          errorMessage <<- error$message
        }
      )

      if (errorMessage != "") {
        connectionStatusLog$ERROR("cdmDatabaseSchema connection", errorMessage)
      } else {
        connectionStatusLog$SUCCESS(
          "cdmDatabaseSchema connection",
          "Connected to cdm:", CDMInfo$cdm_source_name, "Version: ", CDMInfo$ cdm_version
        )
      }


      # update status
      private$.vocabularyInfo <- vocabularyInfo
      private$.CDMInfo <- CDMInfo
      private$.connectionStatusLog <- connectionStatusLog
      private$.getTblVocabularySchema <- getTblVocabularySchema
      private$.getTblCDMSchema <- getTblCDMSchema
    }
  )
)


#' createCDMdbHandlerFromList
#'
#' A function to create a CDMdbHandler object from a list of configuration settings.
#'
#' @param config A list containing configuration settings for the CDMdbHandler.
#'   - databaseName: The name of the database.
#'   - connection: A list of connection details settings.
#'   - cdm: A list of CDM database schema settings.
#'   - cohortTable: The name of the cohort table.
#' @param loadConnectionChecksLevel The level of checks to perform when loading the connection.
#'
#' @return A CDMdbHandler object.
#'
#' @importFrom checkmate assertList assertNames
#'
#' @export
createCDMdbHandlerFromList <- function(
    config,
    loadConnectionChecksLevel = "allChecks") {
  # check parameters
  config |> checkmate::assertList()
  config |>
    names() |>
    checkmate::assertNames(must.include = c("database", "connection", "cdm"))

  # create connectionHandler
  connectionHandler <- connectionHandlerFromList(config$connection)

  # create CDMdbHandler
  CDMdb <- CDMdbHandler$new(
    databaseId = config$database$databaseId,
    databaseName = config$database$databaseName,
    databaseDescription = config$database$databaseDescription,
    connectionHandler = connectionHandler,
    cdmDatabaseSchema = config$cdm$cdmDatabaseSchema,
    vocabularyDatabaseSchema = config$cdm$vocabularyDatabaseSchema,
    resultsDatabaseSchema = if (!is.null(config$cdm$resultsDatabaseSchema)) config$cdm$resultsDatabaseSchema else config$cdm$cdmDatabaseSchema,
    loadConnectionChecksLevel = loadConnectionChecksLevel
  )

  return(CDMdb)
}
