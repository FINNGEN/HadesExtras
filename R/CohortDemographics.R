
#' executedemographicsCounts
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

executeCohortDemographicsCounts <- function(
    exportFolder,
    cohortTableHandler = NULL,
    cohortIds,
    referenceYears = c("cohort_start_date", "cohort_end_date", "birth_datetime"),
    groupBy = c("calendarYear", "ageGroup", "gender"),
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
  groups <- c("calendarYear", "ageGroup", "gender")
  validReferenceYears <- c("cohort_start_date", "cohort_end_date", "birth_datetime")

  exportFolder |> checkmate::assertDirectoryExists()
  cohortIds |> checkmate::assertNumeric()
  minCellCount |> checkmate::assertNumeric()
  groupBy |> checkmate::assertCharacter(min.len = 1)
  groupBy |> checkmate::assertSubset(groups)
  referenceYears |> checkmate::assertCharacter(min.len = 1)
  referenceYears |> checkmate::assertSubset(validReferenceYears)

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
  demographicsCounts  <- getCohortDemographicsCounts(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohortIds,
    referenceYears = referenceYears
  )


  if (length(groupBy) < length(groups)) {
    demographicsCounts <- demographicsCounts |>
      dplyr::group_by(dplyr::across(dplyr::all_of(c("cohortId", groupBy)))) |>
      dplyr::summarize(count = sum(count)) |>
      dplyr::ungroup()

    demographicsCounts <- demographicsCounts |>
      dplyr::bind_cols(
       tibble::tibble(
         calendarYear = NA_real_,
         ageGroup = NA_character_,
         gender = NA_character_
       ) |>
         dplyr::select(dplyr::all_of(setdiff(groups, groupBy)))
      )
  }

  if (minCellCount > 0) {
    demographicsCounts <- demographicsCounts |>
      dplyr::filter(count >= minCellCount)
  }

  demographicsCounts  <-  demographicsCounts  |>
    dplyr::ungroup() |>
    dplyr::transmute(
      database_id = databaseId,
      cohort_id = cohortId,
      reference_year = referenceYear,
      calendar_year = calendarYear,
      age_group = ageGroup,
      gender = gender,
      count = count
    )

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
  dplyr::mutate(databaseId = databaseId) |>
    dplyr::select(databaseId, cohortId, cohortName, sql, json, subsetParent, isSubset, subsetDefinitionId) |>
    .writeToCsv(
      fileName = file.path(exportFolder, "cohort.csv")
    )

  # cohort counts ------------------------------------------------
  cohortTableHandler$getCohortCounts() |>
    dplyr::transmute(
      cohort_id = cohortId,
      cohort_entries = cohortEntries,
      cohort_subjects = cohortSubjects,
      database_id = databaseId
    ) |> .writeToCsv(
      fileName = file.path(exportFolder, "cohort_count.csv")
    )

  # demographicsCountsCounts ------------------------------------------------
  demographicsCounts |>
    .writeToCsv(
        fileName = file.path(exportFolder, "demographics_counts.csv")
    )


  ParallelLogger::logInfo("Results exported")
  return(TRUE)

}


getCohortDemographicsCounts <- function(
    connectionDetails = NULL,
    connection = NULL,
    cdmDatabaseSchema,
    vocabularyDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable = "cohort",
    cohortIds = c(),
    referenceYears = c("cohort_start_date", "cohort_end_date", "birth_datetime")
) {
  #
  # Validate parameters
  #
  validReferenceYears <- c("cohort_start_date", "cohort_end_date", "birth_datetime")

  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  checkmate::assertCharacter(cohortDatabaseSchema, len = 1)
  checkmate::assertString(cohortTable)
  checkmate::assertNumeric(cohortIds, null.ok = TRUE )
  referenceYears |> checkmate::assertCharacter(min.len = 1)
  referenceYears |> checkmate::assertSubset(validReferenceYears)


  #
  # Function
  demographicsCounts <- tibble()
  for (referenceYear in referenceYears) {
    sql <- SqlRender::readSql(system.file("sql/sql_server/CalculateCohortDemographics.sql", package = "HadesExtras", mustWork = TRUE))

    sql <- SqlRender::render(
      sql = sql,
      cdm_database_schema = cdmDatabaseSchema,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      cohort_ids = cohortIds,
      reference_year = referenceYear,
      warnOnMissingParameters = TRUE
    )
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = connection@dbms
    )

    demographicsCounts  <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE) |>
      tibble::as_tibble() |>
      dplyr::mutate(referenceYear = referenceYear ) |>
      dplyr::select(referenceYear, everything()) |>
      dplyr::bind_rows(demographicsCounts)
  }

  demographicsCounts  <- demographicsCounts |>
    tibble::as_tibble() |>
    dplyr::mutate(
      ageGroup = case_when(
        ageGroup <= 0 ~ "0-9",
        ageGroup <= 1 ~ "10-19",
        ageGroup <= 2 ~ "20-29",
        ageGroup <= 3 ~ "30-39",
        ageGroup <= 4 ~ "40-49",
        ageGroup <= 5 ~ "50-59",
        ageGroup <= 6 ~ "60-69",
        ageGroup <= 7 ~ "70-79",
        ageGroup <= 8 ~ "80-89",
        ageGroup <= 9 ~ "90-99",
        TRUE ~ "100+"
      ),
      gender = dplyr::case_when(
        genderConceptId == 8507 ~ "Male",
        genderConceptId == 8532 ~ "Female",
        TRUE ~ "Unknown"
      )
    ) |>
    dplyr::select(-genderConceptId) |>
    dplyr::group_by(referenceYear, cohortId, calendarYear, ageGroup, gender) |>
    dplyr::summarize( count = sum(cohortCount) )


  return(demographicsCounts)
}

















