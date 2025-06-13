#' CohortTableHandler
#'
#' @description
#' Class for handling cohort tables in a CDM database.
#' Inherits from CDMdbHandler.
#'
#' @field cohortDatabaseSchema Schema where cohort tables are stored
#' @field cohortTableNames Names of the cohort tables in the database
#' @field incrementalFolder Path to folder for incremental operations
#' @field cohortDefinitionSet Set of cohort definitions
#' @field cohortGeneratorResults Results from cohort generation process
#' @field cohortDemograpics Demographic information for cohorts
#' @field cohortsOverlap Information about overlapping cohorts
#'
#' @param databaseId ID of the database to connect to
#' @param loadConnectionChecksLevel Level of checks to perform during connection
#' @param newCohortName New name to assign to the cohort
#' @param newShortName New short name to assign to the cohort
#' @param cohortDefinitionSet Set of cohort definitions to use
#' @param incrementalFolder Path to folder for incremental operations
#' @param cohortDatabaseSchema Schema name where cohort tables are stored
#'
#' @importFrom R6 R6Class
#' @importFrom checkmate assertClass assertString
#' @importFrom CohortGenerator createEmptyCohortDefinitionSet createCohortTables getCohortTableNames generateCohortSet getCohortCounts dropCohortStatsTables
#' @importFrom dplyr bind_rows filter left_join count collect nest_by mutate select
#' @importFrom stringr str_remove_all
#'
#' @export
#'
CohortTableHandler <- R6::R6Class(
  classname = "CohortTableHandler",
  inherit = CDMdbHandler,
  private = list(
    # database parameters
    .cohortDatabaseSchema = NULL,
    .cohortTableNames = NULL,
    .incrementalFolder = NULL,
    # Internal cohorts data
    .cohortDefinitionSet = NULL,
    .cohortGeneratorResults = NULL,
    .cohortDemograpics = NULL,
    .cohortsOverlap = NULL
  ),
  active = list(
    # Read-only parameters
    # database parameters
    cohortDatabaseSchema = function() {
      return(private$.cohortDatabaseSchema)
    },
    cohortTableNames = function() {
      return(private$.cohortTableNames)
    },
    incrementalFolder = function() {
      return(private$.incrementalFolder)
    },
    # Internal cohorts data
    cohortDefinitionSet = function() {
      return(private$.cohortDefinitionSet)
    },
    cohortGeneratorResults = function() {
      return(private$.cohortGeneratorResults)
    },
    cohortDemograpics = function() {
      return(private$.cohortDemograpics)
    },
    cohortsOverlap = function() {
      return(private$.cohortsOverlap)
    }
  ),
  public = list(
    #' @description
    #' Initialize the CohortTableHandler object
    #'
    #' @param connectionHandler The connection handler object.
    #' @param databaseId             A text id for the database the it connects to
    #' @param databaseName           A text name for the database the it connects to
    #' @param databaseDescription    A text description for the database the it connects to
    #' @param cdmDatabaseSchema Name of the CDM database schema.
    #' @param vocabularyDatabaseSchema Name of the vocabulary database schema. Default is the same as the CDM database schema.
    #' @param cohortDatabaseSchema Name of the cohort database schema.
    #' @param cohortTableName Name of the cohort table.
    #' @param loadConnectionChecksLevel     (Optional) Level of checks to perform when loading the connection (default is "allChecks")
    initialize = function(connectionHandler,
                          databaseId,
                          databaseName,
                          databaseDescription,
                          cdmDatabaseSchema,
                          vocabularyDatabaseSchema = cdmDatabaseSchema,
                          cohortDatabaseSchema,
                          cohortTableName,
                          loadConnectionChecksLevel = "allChecks") {
      checkmate::assertClass(connectionHandler, "ConnectionHandler")
      checkmate::assertString(cdmDatabaseSchema)
      checkmate::assertString(vocabularyDatabaseSchema)
      checkmate::assertString(cohortDatabaseSchema)
      checkmate::assertString(cohortTableName)

      private$.cohortDatabaseSchema <- cohortDatabaseSchema
      # add timestamp to cohortTableName if it contains <timestamp>
      timestamp <- as.character(as.numeric(format(Sys.time(), "%d%m%Y%H%M%OS2")) * 100)
      if (cohortTableName |> stringr::str_detect("<timestamp>")) {
        cohortTableName <- cohortTableName |> stringr::str_replace("<timestamp>", timestamp)
      }
      private$.cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTableName)
      private$.incrementalFolder <- file.path(tempdir(), timestamp)

      private$.cohortDefinitionSet <- tibble::tibble(
        cohortId = 0,
        cohortName = "", shortName = "",
        sql = "", json = "",
        subsetParent = 0, isSubset = TRUE, subsetDefinitionId = 0,
        .rows = 0
      )

      private$.cohortGeneratorResults <- tibble::tibble(cohortId = 0, buildInfo = list(), .rows = 0)
      private$.cohortDemograpics <- tibble::tibble(cohortId = 0, cohortEntries = 0L, cohortSubjects = 0L, .rows = 0)
      private$.cohortsOverlap <- tibble::tibble(numberOfSubjects = 0, .rows = 0)

      # self$loadConnection()
      # super$initialize is calling self$loadConnection(), self$loadConnection() is calling super$loadConnection()

      super$initialize(
        databaseId = databaseId,
        databaseName = databaseName,
        databaseDescription = databaseDescription,
        connectionHandler = connectionHandler,
        cdmDatabaseSchema = cdmDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        loadConnectionChecksLevel = loadConnectionChecksLevel
      )
    },
    #' Finalize method
    #' @description
    #' Closes the connection if active.
    finalize = function() {
      CohortGenerator_dropCohortStatsTables(
        connection = self$connectionHandler$getConnection(),
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTableNames = self$cohortTableNames
      )
      unlink(private$.incrementalFolder, recursive = TRUE)

      super$finalize()
    },
    #'
    #' loadConnection
    #' @description
    #' Reloads the connection with the initial setting and updates connection status
    loadConnection = function(loadConnectionChecksLevel) {
      if (loadConnectionChecksLevel == "dontConnect") {
        private$.connectionStatusLog <- connectionStatusLog
        return()
      }

      super$loadConnection(loadConnectionChecksLevel)

      # Check cohort database schema
      errorMessage <- ""
      tryCatch(
        {
          CohortGenerator_createCohortTables(
            connection = self$connectionHandler$getConnection(),
            cohortDatabaseSchema = self$cohortDatabaseSchema,
            cohortTableNames = self$cohortTableNames
          )
        },
        error = function(error) {
          errorMessage <<- error$message
        }
      )
      if (errorMessage != "") {
        private$.connectionStatusLog$ERROR("Create cohort tables", errorMessage)
      } else {
        private$.connectionStatusLog$SUCCESS("Create cohort tables", "Created cohort tables")
      }
    },
    #'
    #' insertOrUpdateCohorts
    #' @description
    #' If there is no cohort with the same cohortId it is added to the cohortDefinitionSet,
    #' If there is a cohort with the same cohortId, the cohort is updated in the cohortDefinitionSet
    #' CohortDefinitionSet is generated and demographics is updated for only the cohorts that have changed
    #'
    #' @param cohortDefinitionSet The cohort definition set to add.
    insertOrUpdateCohorts = function(cohortDefinitionSet) {
      #
      # Check parameters
      #
      if (!CohortGenerator::isCohortDefinitionSet(cohortDefinitionSet)) {
        stop("Provided table is not of cohortDefinitionSet format")
      }

      # if not shortName, create it
      if (!"shortName" %in% names(cohortDefinitionSet)) {
        cohortDefinitionSet$shortName <- paste0("C", cohortDefinitionSet$cohortId)
      } else {
        cohortDefinitionSet$shortName <- dplyr::if_else(is.na(cohortDefinitionSet$shortName), paste0("C", cohortDefinitionSet$cohortId), cohortDefinitionSet$shortName)
      }

      cohortIdsExists <- intersect(private$.cohortDefinitionSet$cohortId, cohortDefinitionSet$cohortId)
      if (length(cohortIdsExists) != 0) {
        warning("Following cohort ids already exists on the cohort table and will be updated: ", paste(cohortIdsExists, collapse = ", "))
      }

      #
      # Function
      #
      # update existing cohorts in cohortDefinitionSet
      hasSubSets <- isTRUE(attr(private$.cohortDefinitionSet, "hasSubsetDefinitions")) | isTRUE(attr(cohortDefinitionSet, "hasSubsetDefinitions"))
      cohortDefinitionSet <- dplyr::bind_rows(
        private$.cohortDefinitionSet |> dplyr::filter(!(cohortId %in% cohortIdsExists)),
        cohortDefinitionSet
      ) |>
        dplyr::arrange(cohortId)

      # TODO: fix cohortDefinitionSet for subsets using
      cohortDefinitionSet <- cohortDefinitionSet |>
        dplyr::mutate(
          subsetParent = dplyr::if_else( is.na(isSubset) | isSubset==FALSE, cohortId, subsetParent),
          isSubset=dplyr::if_else(is.na(isSubset), FALSE,isSubset)
        )
      attr(cohortDefinitionSet, "hasSubsetDefinitions") <- hasSubSets

      # generate cohorts in incremental mode
      cohortGeneratorResults <- CohortGenerator_generateCohortSet(
        connection = self$connectionHandler$getConnection(),
        cdmDatabaseSchema = self$cdmDatabaseSchema,
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTableNames = self$cohortTableNames,
        cohortDefinitionSet = cohortDefinitionSet,
        incremental = TRUE,
        incrementalFolder = private$.incrementalFolder
      ) |>
        dplyr::arrange(cohortId)

      # keep only these that have changed
      cohortGeneratorResultsToUpdate <- cohortGeneratorResults |>
        dplyr::filter(generationStatus == "COMPLETE")

      # Update cohortDemograpics
      cohortDemograpicsToUpdate <- tibble::tibble(cohortId = 0, .rows = 0)
      if (length(cohortGeneratorResultsToUpdate$cohortId) != 0) {
        cohortDemograpicsToUpdate <- CohortGenerator_getCohortDemograpics(
          connection = self$connectionHandler$getConnection(),
          cdmDatabaseSchema = self$cdmDatabaseSchema,
          vocabularyDatabaseSchema = self$vocabularyDatabaseSchema,
          cohortDatabaseSchema = self$cohortDatabaseSchema,
          cohortTable = self$cohortTableNames$cohortTable,
          cohortIds = cohortGeneratorResultsToUpdate$cohortId
        )
      }

      # update changes
      cohortGeneratorResults <- dplyr::bind_rows(
        private$.cohortGeneratorResults |> dplyr::filter(!(cohortId %in% cohortGeneratorResultsToUpdate$cohortId)),
        cohortGeneratorResultsToUpdate
      ) |>
        dplyr::arrange(cohortId)

      # update cohortDemograpics
      cohortDemograpics <- dplyr::bind_rows(
        private$.cohortDemograpics |> dplyr::filter(!(cohortId %in% cohortDemograpicsToUpdate$cohortId)),
        cohortDemograpicsToUpdate
      ) |>
        dplyr::arrange(cohortId)

      # update cohortsOverlap
      cohortsOverlap <- CohortGenerator_getCohortsOverlaps(
        connection = self$connectionHandler$getConnection(),
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTable = self$cohortTableNames$cohortTable
      )

      # if no errors save
      private$.cohortDefinitionSet <- cohortDefinitionSet
      private$.cohortGeneratorResults <- cohortGeneratorResults
      private$.cohortDemograpics <- cohortDemograpics
      private$.cohortsOverlap <- cohortsOverlap
    },
    #'
    #' deleteCohorts
    #' @description
    #' Deletes cohorts from the cohort table.
    #'
    #' @param cohortIds The cohort ids to delete.
    deleteCohorts = function(cohortIds) {
      # check parameters
      cohortIdsNotExists <- setdiff(cohortIds, private$.cohortDefinitionSet$cohortId)
      if (length(cohortIdsNotExists) != 0) {
        stop("Following cohort ids dont exists on the cohort table: ", paste(cohortIdsNotExists, collapse = ", "))
      }

      # function
      CohortGenerator_deleteCohortFromCohortTable(
        connection = self$connectionHandler$getConnection(),
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTableNames = self$cohortTableNames,
        cohortIds = cohortIds,
        incrementalFolder = private$.incrementalFolder
      )

      private$.cohortDefinitionSet <- private$.cohortDefinitionSet |>
        dplyr::filter(!(cohortId %in% cohortIds))

      private$.cohortGeneratorResults <- private$.cohortGeneratorResults |>
        dplyr::filter(!(cohortId %in% cohortIds))

      private$.cohortDemograpics <- private$.cohortDemograpics |>
        dplyr::filter(!(cohortId %in% cohortIds))

      private$.cohortsOverlap <- private$.cohortsOverlap |>
        removeCohortIdsFromCohortOverlapsTable(cohortIds)
    },
    #'
    #' getCohortCounts
    #' @description
    #' Retrieves cohort counts from the cohort table.
    #'
    #' @return A tibble containing the cohort counts with names.
    getCohortCounts = function() {
      cohortCountsWithNames <- private$.cohortDefinitionSet |>
        dplyr::select(cohortName, cohortId) |>
        dplyr::left_join(
          private$.cohortDemograpics |> dplyr::select(cohortId, cohortEntries, cohortSubjects),
          by = "cohortId"
        )
      return(cohortCountsWithNames)
    },
    #'
    #' getNumberOfSubjects
    #' @description
    #'  Retrieves the number of subjects of a cohort ID.
    #' @param selected_cohortId The cohort id for which number of subjects is sought.
    #' @return A numeric value of the number of subjects in a cohort.
    getNumberOfSubjects = function(selected_cohortId) {
      nSubjectsInCohort <- self$getCohortCounts() |>
        dplyr::filter(cohortId == selected_cohortId) |>
        dplyr::pull(cohortSubjects)
      return(nSubjectsInCohort)
    },
    #'
    #' getNumberOfCohortEntries
    #' @description
    #'  Retrieves the number of entries of a cohort ID.
    #' @param selected_cohortId The cohort id for which number of entries is sought.
    #' @return A numeric value of the number of entries in a cohort.
    getNumberOfCohortEntries = function(selected_cohortId) {
      nEntriesInCohort <- self$getCohortCounts() |>
        dplyr::filter(cohortId == selected_cohortId) |>
        dplyr::pull(cohortEntries)
      return(nEntriesInCohort)
    },
    #'
    #' getCohortsSummary
    #' @description
    #' Retrieves the summary of cohorts including cohort start and end year histograms and sex counts.
    #'
    #' @return A tibble containing cohort summary.
    getCohortsSummary = function(includeAllEvents=F) {

      cohortDemographics <- private$.cohortDemograpics
      if (!includeAllEvents) {
        cohortDemographics <- cohortDemographics |>
          dplyr::select(-dplyr::any_of(c("histogramBirthYearAllEvents", "sexCountsAllEvents")))
      }

      cohortsSummaryWithNames <- private$.cohortDefinitionSet |>
        dplyr::select(cohortName, shortName, cohortId) |>
        dplyr::mutate(
          databaseId = super$databaseId,
          databaseName = super$databaseName
        ) |>
        dplyr::left_join(
          cohortDemographics,
          by = "cohortId"
        ) |>
        dplyr::left_join(
          private$.cohortGeneratorResults |> dplyr::select(cohortId, buildInfo),
          by = "cohortId"
        ) |>
        correctEmptyCohortsInCohortsSummary()

      return(cohortsSummaryWithNames)
    },
    #'
    #' getCohortIdAndNames
    #' @description
    #' Retrieves the cohort names.
    #'
    #' @return A vector with the name of the cohorts
    getCohortIdAndNames = function() {
      return(private$.cohortDefinitionSet |> dplyr::select(cohortName, shortName, cohortId, subsetDefinitionId))
    },
    #'
    #' updateCohortNames
    #' @description
    #' Updates the cohort name and short name.
    #' @param cohortId The cohort id to update.
    #' @param cohortName The new cohort name.
    #' @param shortName The new short name.
    #'
    updateCohortNames = function(cohortId, newCohortName, newShortName) {
      # check parameters
      if (!cohortId %in% private$.cohortDefinitionSet$cohortId) {
        stop("Cohort id ", cohortId, " does not exist in the cohort table")
      }

      # function
      private$.cohortDefinitionSet <- private$.cohortDefinitionSet |>
        dplyr::mutate(
          cohortName = dplyr::if_else(cohortId == {{ cohortId }}, newCohortName, cohortName),
          shortName = dplyr::if_else(cohortId == {{ cohortId }}, newShortName, shortName)
        )
    },
    #'
    #' getCohortsOverlap
    #' @description
    #' Retrieves the number of subjects that are in more than one cohort.
    #' @return A tibble containing one logical column for each cohort with name a cohort id,
    #' and an additional column `numberOfSubjects` with the number of subjects in the cohorts combination.
    #'
    getCohortsOverlap = function() {
      return(private$.cohortsOverlap)
    },
    #'
    #' getNumberOfOverlappingSubjects
    #' @description
    #' Retrieves the number of subjects that are overlapping between two given cohorts.
    #' @param selected_cohortId1 The cohort id of the first cohort.
    #' @param selected_cohortId2 The cohort id of the second cohort.
    #' @return A numeric value of the number of overlapping subjects between two given cohorts.
    #'
    getNumberOfOverlappingSubjects = function(selected_cohortId1,selected_cohortId2) {
      nSubjectsOverlap <- private$.cohortsOverlap |>
        dplyr::filter(
          stringr::str_detect(cohortIdCombinations, paste0("-", selected_cohortId1, "-")) &
            stringr::str_detect(cohortIdCombinations, paste0("-", selected_cohortId2, "-"))
        ) |>
        dplyr::pull(numberOfSubjects)  |>
        sum()

      return(nSubjectsOverlap)
    },
    #'
    #' getSexFisherTest
    #' @description
    #' Compares the proportion of males and females in two cohorts using Fisher's exact test.
    #' @param selected_cohortId1 The cohort id of the first cohort.
    #' @param selected_cohortId2 The cohort id of the second cohort.
    #' @return a list with class R htest class containing components such as p.value and conf.int of the test
    #'
    getSexFisherTest = function(selected_cohortId1,selected_cohortId2,testFor="Subjects") {

      if(testFor == "allEvents"){
         sexCase <- self$getCohortsSummary(includeAllEvents=T) |>
          dplyr::filter(cohortId == selected_cohortId1) |>
          dplyr::pull(sexCountsAllEvents)
        sexControl <-  self$getCohortsSummary(includeAllEvents=T) |>
          dplyr::filter(cohortId == selected_cohortId2) |>
          dplyr::pull(sexCountsAllEvents)
      }else {
        sexCase <- self$getCohortsSummary() |>
          dplyr::filter(cohortId == selected_cohortId1) |>
          dplyr::pull(sexCounts)
        sexControl <-  self$getCohortsSummary() |>
          dplyr::filter(cohortId == selected_cohortId2) |>
          dplyr::pull(sexCounts)
      }

      nMaleCases <- sexCase[[1]]  |> dplyr::filter(sex == "MALE")  |> dplyr::pull(n)
      nMaleCases <- ifelse(length(nMaleCases)==0, 0, nMaleCases)
      nFemaleCases <- sexCase[[1]]  |> dplyr::filter(sex == "FEMALE")  |> dplyr::pull(n)
      nFemaleCases <- ifelse(length(nFemaleCases)==0, 0, nFemaleCases)
      nMaleControls <- sexControl[[1]]  |> dplyr::filter(sex == "MALE") |> dplyr::pull(n)
      nMaleControls <- ifelse(length(nMaleControls)==0, 0, nMaleControls)
      nFemaleControls <- sexControl[[1]]  |> dplyr::filter(sex == "FEMALE") |> dplyr::pull(n)
      nFemaleControls <- ifelse(length(nFemaleControls)==0, 0, nFemaleControls)

      data <-matrix(c(nMaleCases,nFemaleCases,nMaleControls,nFemaleControls),ncol=2)
      fisher_results <- stats::fisher.test(data)

      return(fisher_results)
    },
    #'
    #' getYearOfBirthTests
    #' @description
    #' Compares the year of birth distributions between the case and control cohorts using two-sample t-test to compare mean year of birth,
    #' uses cohen's d to assess effect size of the difference in the mean of the year of births,and the Kolmogorov-Smirnov test
    #' to evaluate if year of births in the two cohorts have similar distribution.
    #' @param selected_cohortId1 The cohort id of the first cohort.
    #' @param selected_cohortId2 The cohort id of the second cohort.
    #' @return a list with with three members ttestResult (R htest object), kstestResult (R htest object), cohendresult (list of meanInCases, meanInControls, pooledsd, and cohend)
    #'
    getYearOfBirthTests = function(selected_cohortId1,selected_cohortId2,testFor="Subjects") {

      if(testFor == "allEvents"){
        yearOfBirthCase <- self$getCohortsSummary(includeAllEvents=T) |>
          dplyr::filter(cohortId == selected_cohortId1) |>
          dplyr::pull(histogramBirthYearAllEvents)
        yearOfBirthControl <- self$getCohortsSummary(includeAllEvents=T) |>
          dplyr::filter(cohortId == selected_cohortId2) |>
          dplyr::pull(histogramBirthYearAllEvents)
      }else{
        yearOfBirthCase <- self$getCohortsSummary() |>
          dplyr::filter(cohortId == selected_cohortId1) |>
          dplyr::pull(histogramBirthYear)
        yearOfBirthControl <- self$getCohortsSummary() |>
          dplyr::filter(cohortId == selected_cohortId2) |>
          dplyr::pull(histogramBirthYear)
      }

      cases <- unlist(yearOfBirthCase[[1]] |> tidyr::uncount(n))
      controls <- unlist(yearOfBirthControl[[1]] |> tidyr::uncount(n))

      # handle small size cases
      if (length(cases[!is.na(cases)]) < 2 || length(controls[!is.na(controls)]) < 2) {
        return(list(
          ttestResult = NA,
          ksResult = NA,
          cohendResult = c(
            meanInCases = NA,
            meanInControls = NA,
            pooledsd = NA,
            cohend = NA
          )
        ))
      }


      ttestResult <-  suppressWarnings(suppressMessages(
        t.test(cases[!is.na(cases)], controls[!is.na(controls)])
      ))

      ks_result <- suppressWarnings(suppressMessages(
        ks.test(cases[!is.na(cases)], controls[!is.na(controls)])
      ))

      # Calculate Cohen's d
      meanCases = mean(cases,na.rm = TRUE)
      meanControls = mean(controls,na.rm = TRUE)
      mean_diff <- meanCases - meanControls
      pooled_sd <- sqrt(((length(cases)-1)*var(cases,na.rm = TRUE) + (length(controls)-1)*var(controls,na.rm = TRUE)) / (length(cases) + length(controls) - 2))
      cohen_d <- mean_diff / pooled_sd
      cohendresult =  c(meanInCases = meanCases, meanInControls = meanControls,pooledsd = pooled_sd, cohend=cohen_d)

      return(list(ttestResult = ttestResult, ksResult = ks_result,cohendResult = cohendresult))
    }
  )
)


#' createCohortTableHandlerFromList
#'
#' A function to create a CohortTableHandler object from a list of cohortTableHandlerConfiguration settings.
#'
#' @param cohortTableHandlerConfig A list containing cohortTableHandlerConfiguration settings for the CohortTableHandler.
#'   - databaseName: The name of the database.
#'   - connection: A list of connection details settings.
#'   - cdm: A list of CDM database schema settings.
#'   - cohortTable: A list of cohort table settings.
#' @param loadConnectionChecksLevel (Optional) Level of checks to perform when loading the connection (default is "allChecks").
#'
#' @return A CohortTableHandler object.
#'
#' @importFrom checkmate assertList assertNames
#'
#' @export
createCohortTableHandlerFromList <- function(
    cohortTableHandlerConfig,
    loadConnectionChecksLevel = "allChecks") {
  cohortTableHandlerConfig |> checkmate::assertList()
  cohortTableHandlerConfig |>
    names() |>
    checkmate::assertSubset(c("database", "connection", "cdm", "cohortTable"))

  # create connectionHandler
  connectionHandler <- connectionHandlerFromList(cohortTableHandlerConfig$connection)

  # create cohortTableHandler
  cohortTableHandler <- CohortTableHandler$new(
    connectionHandler = connectionHandler,
    databaseId = cohortTableHandlerConfig$database$databaseId,
    databaseName = cohortTableHandlerConfig$database$databaseName,
    databaseDescription = cohortTableHandlerConfig$database$databaseDescription,
    cdmDatabaseSchema = cohortTableHandlerConfig$cdm$cdmDatabaseSchema,
    vocabularyDatabaseSchema = cohortTableHandlerConfig$cdm$vocabularyDatabaseSchema,
    cohortDatabaseSchema = cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema,
    cohortTableName = cohortTableHandlerConfig$cohortTable$cohortTableName,
    loadConnectionChecksLevel = loadConnectionChecksLevel
  )

  return(cohortTableHandler)
}

