
#' CohortDiagnostics_runTimeCodeWAS
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
#' @importFrom CohortDiagnostics saveDatabaseMetaData
#'
#' @export
#'

executeTimeCodeWAS <- function(
    exportFolder,
    cohortTableHandler,
    cohortIdCases,
    cohortIdControls,
    covariateSettings = FeatureExtraction::createDefaultTemporalCovariateSettings(),
    minCellCount = 1
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
  cohortTableHandler |> checkmate::assertR6(class = "CohortTableHandler")
  cohortIdCases |> checkmate::assertNumeric()
  cohortIdControls |> checkmate::assertNumeric()
  covariateSettings |> checkmate::assertList()


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
  ParallelLogger::logInfo("Getting FeatureExtraction for cases")
  covariate_case <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTable,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortId = cohortIdCases,
    aggregated = T
  )
  ParallelLogger::logInfo("Getting FeatureExtraction for controls")
  covariate_control <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTable,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortId = cohortIdControls,
    aggregated = T
  )

  ParallelLogger::logInfo("calcualting number of subjects with observation case and controls in each time window")

  cohortTbl <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, cohortTable))
  observationPeriodTbl <- dplyr::tbl(connection, tmp_inDatabaseSchema(cdmDatabaseSchema, "observation_period"))

  timeWindows <- tibble(
    id_window =  as.integer(1:length(covariateSettings$temporalStartDays)),
    start_day = as.integer(covariateSettings$temporalStartDays),
    end_day = as.integer(covariateSettings$temporalEndDays)
  )
  timeWindowsTbl <- tmp_dplyr_copy_to(connection, timeWindows, overwrite = TRUE)

  windowCounts <- cohortTbl |>
    dplyr::filter(cohort_definition_id %in% c(cohortIdCases, cohortIdControls)) |>
    dplyr::left_join(
      # at the moment take the first and last
      observationPeriodTbl |>
        dplyr::select(person_id, observation_period_start_date, observation_period_end_date) |>
        dplyr::group_by(person_id) |>
        dplyr::summarise(
          observation_period_start_date = min(observation_period_start_date, na.rm = TRUE),
          observation_period_end_date = max(observation_period_end_date, na.rm = TRUE)
        ),
      by = c("subject_id" = "person_id")) |>
    dplyr::cross_join(timeWindowsTbl) |>
    dplyr::filter(
      # exclude if window is under the observation_period_start_date or over the observation_period_end_date
      !(dateAdd("day", end_day, cohort_start_date) < observation_period_start_date |
          dateAdd("day", start_day, cohort_start_date) > observation_period_end_date )
    ) |>
    dplyr::group_by(cohort_definition_id, id_window) |>
    dplyr::count() |>
    dplyr::collect()



  windowCounts <-  windowCounts |>
    dplyr::mutate(cohort_definition_id = dplyr::case_when(
      cohort_definition_id == cohortIdCases ~ "n_cases",
      cohort_definition_id == cohortIdControls ~ "n_controls",
      TRUE ~ as.character(NA)
    )) |>
    tidyr::spread(key = cohort_definition_id, n)|>
    dplyr::rename(timeId = id_window)


  ParallelLogger::logInfo("Calculating time counts")

  timeCodeWasCounts <-
    dplyr::full_join(
      covariate_case$covariates |> tibble::as_tibble() |>
        select(covariateId, timeId, n_cases_yes=sumValue),
      covariate_control$covariates |> tibble::as_tibble() |>
        select( covariateId, timeId, n_controls_yes=sumValue),
      by = c("covariateId", "timeId")
    )  |>
    dplyr::left_join(
      windowCounts,
      by="timeId"
    ) |>
    dplyr::mutate(
      n_cases_yes = dplyr::if_else(is.na(n_cases_yes), 0, n_cases_yes),
      n_controls_yes = dplyr::if_else(is.na(n_controls_yes), 0, n_controls_yes),
      n_cases_no = n_cases - n_cases_yes,
      n_controls_no = n_controls - n_controls_yes
    ) |>
    dplyr::select(-n_cases, -n_controls)


  ParallelLogger::logInfo("Adding Fisher test output")
  timeCodeWasCounts <- .addFisherTestToCodeCounts(timeCodeWasCounts)

  ParallelLogger::logInfo("CohortDiagnostics_runTimeCodeWAS completed")




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
  .writeToCsv(
    data = cohortDefinitionSet,
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

  # timeCodeWasCounts ------------------------------------------------
  timeCodeWasCounts |>
    dplyr::mutate(database_id = databaseId) |>
    dplyr::select(database_id, dplyr::everything()) |>
    .writeToCsv(
      fileName = file.path(exportFolder, "temporal_covariate_timecodewas.csv")
    )

  dplyr::bind_rows(
    covariate_case$covariateRef |> tibble::as_tibble(),
    covariate_control$covariateRef |> tibble::as_tibble()
  ) |>
    dplyr::distinct() |>
    .writeToCsv(
      fileName = file.path(exportFolder, "temporal_covariate_ref.csv")
    )

  dplyr::bind_rows(
    covariate_case$analysisRef |> tibble::as_tibble(),
    covariate_control$analysisRef |> tibble::as_tibble()
  ) |>
    dplyr::distinct() |>
    .writeToCsv(
      fileName = file.path(exportFolder, "temporal_analysis_ref.csv")
    )

  timeWindows |>
    dplyr::rename(timeId = id_window) |>
    .writeToCsv(
      fileName = file.path(exportFolder, "temporal_time_ref.csv")
    )

  ParallelLogger::logInfo("Results exported")
  return(TRUE)

}





#' @title .addFisherTestToCodeCounts
#' @description performs an Fisher test in each row of the case_controls_counts table
#' @param timeCodeWasCounts table obtained from `getCodeCounts`.
#' @return inputed table with appened columsn :
#' - p : for the p-value
#' - OR; for the ods ratio
#' @export
#' @importFrom dplyr setdiff bind_cols select mutate if_else
#' @importFrom purrr pmap_df
.addFisherTestToCodeCounts <- function(timeCodeWasCounts){

  missing_collumns <- dplyr::setdiff(c("n_cases_yes","n_cases_no","n_controls_yes","n_controls_no"), names(timeCodeWasCounts))
  if(length(missing_collumns)!=0){
    stop("case_controls_counts is missing the following columns: ", missing_collumns)
  }


  timeCodeWasCounts <- timeCodeWasCounts |>
    dplyr::bind_cols(
      timeCodeWasCounts |>
        dplyr::select(n_cases_yes,n_cases_no,n_controls_yes,n_controls_no) |>
        purrr::pmap_df( ~.fisher(..1,..2,..3,..4))
    )

  return(timeCodeWasCounts)

}



.fisher <- function(a,b,c,d){
  data <-matrix(c(a,b,c,d),ncol=2)
  fisher_results <- stats::fisher.test(data)
  return(list(
    p_value = fisher_results$p.value,
    odds_ratio = fisher_results$estimate
  ))
}

.writeToCsv <- function(data, fileName){
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  data |> readr::write_csv(file = fileName, na = "")
}


#' csvFilesToSqlite
#'
#' This function reads CSV files from a specified folder and imports them into a SQLite database. It creates tables in the SQLite database based on the specifications provided in a CSV file and uploads the data from the CSV files into these tables.
#'
#' @param dataFolder A character string specifying the folder path where the CSV files are located.
#' @param sqliteDbPath A character string specifying the file path for the SQLite database. Default is "timeCodeWAS.sqlite".
#' @param overwrite A logical value indicating whether to overwrite the existing SQLite database if it already exists. Default is FALSE.
#' @param tablePrefix A character string specifying the prefix to be added to the table names created in the SQLite database.
#'
#' @return A character string indicating the file path of the SQLite database where the data has been imported.
#'
#' @importFrom DatabaseConnector createConnectionDetails connect renderTranslateExecuteSql disconnect
#' @importFrom ResultModelManager generateSqlSchema uploadResults
#' @importFrom readr read_csv
#' @importFrom SqlRender snakeCaseToCamelCase
#'
#' @export
#'
csvFilesToSqlite  <- function(
    dataFolder,
    sqliteDbPath = "timeCodeWAS.sqlite",
    overwrite = FALSE,
    tablePrefix = ""
){

  if (file.exists(sqliteDbPath) & !overwrite) {
    stop("File ", sqliteDbPath, " already exists. Set overwrite = TRUE to replace")
  } else if (file.exists(sqliteDbPath)) {
    unlink(sqliteDbPath)
  }

  specPath <- system.file("settings", "resultsDataModelSpecifications_TimeCodeWAS.csv", package = utils::packageName())
  #specPath <- here::here("inst/settings/resultsDataModelSpecifications_TimeCodeWAS.csv")
  spec <- readr::read_csv(specPath, show_col_types = FALSE)
  colnames(spec) <- SqlRender::snakeCaseToCamelCase(colnames(spec))

  connectionDetails <- DatabaseConnector::createConnectionDetails("sqlite", server = sqliteDbPath)
  connection <- DatabaseConnector::connect(connectionDetails)
  sql <- ResultModelManager::generateSqlSchema(schemaDefinition = spec)
  DatabaseConnector::renderTranslateExecuteSql(connection, sql, database_schema = "main", table_prefix = tablePrefix)


  ResultModelManager::uploadResults(
    connection,
    schema = "main",
    resultsFolder = dataFolder,
    tablePrefix = tablePrefix,
    purgeSiteDataBeforeUploading = FALSE,
    specifications = spec
  )

  DatabaseConnector::disconnect(connection)

  return(sqliteDbPath)

}




























