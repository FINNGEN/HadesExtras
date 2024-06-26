
#' executeCohortOverlaps
#'
#' This function calculates the number of subjects with observation case and controls in each time window for a given cohort. It returns a data frame with the counts of cases and controls for each time window and covariate, as well as the results of a Fisher's exact test comparing the counts of cases and controls.
#'
#' @param exportFolder A character string specifying the folder path where the results will be exported.
#' @param cohortTableHandler An object of class 'CohortTableHandler' representing the cohort table handler.
#' @param cohortIdCases The ID of the cohort representing cases.
#' @param cohortIdControls The ID of the cohort representing controls.
#' @param covariateSettings A list of settings for extracting temporal covariates.
#' @param minCellCount An integer specifying the minimum cell count for Fisher's exact test.
#'
#' @return A logical value indicating if the function was executed successfully.
#'
#' @importFrom DatabaseConnector connect disconnect dateAdd
#' @importFrom FeatureExtraction getDbCovariateData createDefaultTemporalCovariateSettings
#' @importFrom checkmate assertDirectoryExists assertR6 assertNumeric assertList
#' @importFrom dplyr tbl filter left_join cross_join group_by count collect mutate bind_rows distinct everything
#' @importFrom tidyr spread
#' @importFrom tibble as_tibble
#' @importFrom stringr str_c
#'
#' @export
#'

executeCohortOverlaps <- function(
    exportFolder,
    cohortTableHandler = NULL,
    cohortIds,
    minCellCount = 0
    # # TODO: add these parameters if cohortTableHandler is NULL
    # cohortDefinitionSet = NULL,
    # databaseId = NULL,
    # databaseName = NULL,
    # databaseDescription = NULL,
    # connectionDetails = NULL,
    # connection = NULL,
    # cdmDatabaseSchema = NULL,
    # vocabularyDatabaseSchema = cdmDatabaseSchema,
    # cohortDatabaseSchema = NULL,
    # cohortTable = "cohort",
    # vocabularyVersionCdm = NULL,
    # vocabularyVersion = NULL
) {
  #
  # Check parameters
  #

  exportFolder |> checkmate::assertDirectoryExists()
  cohortIds |> checkmate::assertNumeric()
  minCellCount |> checkmate::assertNumeric()

  cohortTableHandler |> checkmate::assertR6(class = "CohortTableHandler")

  # overwrite temporalCovariateSettings to be only one window
  # covariateSettings$temporalStartDays <- temporalStartDays
  # covariateSettings$temporalEndDays <- temporalEndDays

  # cohortDefinitionSet |> checkmate::assertDataFrame()
  # exportFolder |> checkmate::assertDirectoryExists()
  # databaseId |> checkmate::assertString()
  # databaseName |> checkmate::assertString(null.ok = TRUE)
  # databaseDescription |> checkmate::assertString(null.ok = TRUE)
  #
  #
  # if (is.null(connection) && is.null(connectionDetails)) {
  #   stop("You must provide either a database connection or the connection details.")
  # }
  #
  # if (is.null(connection)) {
  #   connection <- DatabaseConnector::connect(connectionDetails)
  #   on.exit(DatabaseConnector::disconnect(connection))
  # }
  #
  # cdmDatabaseSchema |> checkmate::assertString()
  # vocabularyDatabaseSchema |> checkmate::assertString()
  # cohortDatabaseSchema |> checkmate::assertString()
  # cohortTable |> checkmate::assertString()
  # cohortIdCases |> checkmate::assertNumeric()
  # cohortIdControls |> checkmate::assertNumeric()
  # covariateSettings |> checkmate::assertList()

  connection <- cohortTableHandler$connectionHandler$getConnection()
  cohortTable <- cohortTableHandler$cohortTableNames$cohortTable
  cdmDatabaseSchema <- cohortTableHandler$cdmDatabaseSchema
  cohortDatabaseSchema <- cohortTableHandler$cohortDatabaseSchema
  vocabularyDatabaseSchema <- cohortTableHandler$vocabularyDatabaseSchema
  cohortDefinitionSet <- cohortTableHandler$cohortDefinitionSet
  databaseId <- cohortTableHandler$databaseName
  databaseName <- cohortTableHandler$CDMInfo$cdm_source_abbreviation
  databaseDescription <- cohortTableHandler$CDMInfo$cdm_source_name
  vocabularyVersionCdm <- cohortTableHandler$CDMInfo$cdm_version
  vocabularyVersion <- cohortTableHandler$vocabularyInfo$vocabulary_version


  #
  # function
  #
  cohortIdsToRemove <- cohortTableHandler$getCohortCounts()$cohortId |> setdiff(cohortIds)

  cohortOverlaps <- cohortTableHandler$getCohortsOverlap() |>
    removeCohortIdsFromCohortOverlapsTable(cohortIdsToRemove)  |>
    dplyr::filter(numberOfSubjects >= minCellCount)

  #
  # Export
  #
  ParallelLogger::logInfo("Exporting results")

  # Database metadata ---------------------------------------------
  CohortDiagnostics:::saveDatabaseMetaData(
    databaseId = databaseId,
    databaseName =  databaseName,
    databaseDescription =  databaseDescription,
    exportFolder = exportFolder,
    minCellCount = minCellCount,
    vocabularyVersionCdm = vocabularyVersionCdm,
    vocabularyVersion = vocabularyVersion
  )

  # Cohort data ------------------------------------------------
  cohortDefinitionSet |>
    dplyr::select(cohortId, cohortName, sql, json, subsetParent, isSubset, subsetDefinitionId) |>
    .writeToCsv(
      fileName = file.path(exportFolder, "cohort.csv")
    )

  # cohort counts ------------------------------------------------
  cohortTableHandler$getCohortCounts() |>
    dplyr::mutate(
      cohort_id = cohortId,
      cohort_entries = cohortEntries,
      cohort_subjects = cohortSubjects,
      database_id = databaseId
    ) |> .writeToCsv(
      fileName = file.path(exportFolder, "cohort_counts.csv")
    )

  # CohortOverlapsCounts ------------------------------------------------
  cohortOverlaps |>
    .writeToCsv(
        fileName = file.path(exportFolder, "CohortOverlaps_results.csv")
    )


  ParallelLogger::logInfo("Results exported")
  return(TRUE)

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
#' @examples
#' cohortOverlaps <- data.frame(cohortIdCombinations = c("-1-2-", "-2-3-", "-3-4-"),
#'                             numberOfSubjects = c(10, 15, 20))
#' cohortIds <- c(2, 3)
#' removeCohortIdsFromCohortOverlapsTable(cohortOverlaps, cohortIds)
#' @export
removeCohortIdsFromCohortOverlapsTable <- function(cohortOverlaps, cohortIds) {

  if (length(cohortIds) == 0) {
    return(cohortOverlaps)
  }

  cohortOverlaps |> checkmate::assertDataFrame()
  cohortOverlaps  |> names()  |> checkmate::assertSubset(c('cohortIdCombinations', 'numberOfSubjects'))
  cohortIds |> checkmate::assertNumeric()

  cohortOverlaps <- cohortOverlaps |>
    dplyr::mutate(
      cohortIdCombinations = purrr::map_chr(cohortIdCombinations, ~{
        a <- stringr::str_split(.x, '-')[[1]]   |>
          setdiff(c('', as.character(cohortIds)))  |>
          paste0(collapse = '-')
        a  <- paste0('-', a, '-')
      }),
    ) |>
    dplyr::filter(!stringr::str_detect(cohortIdCombinations, '--')) |>
    dplyr::group_by(cohortIdCombinations) |>
    dplyr::summarize(numberOfSubjects = sum(numberOfSubjects), .groups = "drop")

  return(cohortOverlaps)
}

