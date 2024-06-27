
#' CohortDiagnostics_runTimeCodeWAS
#'
#' This function calculates the number of subjects with observation case and controls in each time window for a given cohort. It returns a data frame with the counts of cases and controls for each time window and covariate, as well as the results of a Fisher's exact test comparing the counts of cases and controls.
#'
#' @param exportFolder A character string specifying the folder path where the results will be exported.
#' @param cohortTableHandler An object of class 'CohortTableHandler' representing the cohort table handler.
#' @param cohortIdCases The ID of the cohort representing cases.
#' @param cohortIdControls The ID of the cohort representing controls.
#' @param analysisIds Numeric vector specifying the IDs of the analyses to be performed.
#' @param temporalStartDays Numeric vector specifying the start days for each time window.
#' @param temporalEndDays Numeric vector specifying the end days for each time window.
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

executeTimeCodeWAS <- function(
    exportFolder,
    cohortTableHandler = NULL,
    cohortIdCases,
    cohortIdControls,
    analysisIds,
    temporalStartDays,
    temporalEndDays,
    minCellCount = 1,
    # # TODO: add these parameters if cohortTableHandler is NULL
    cohortDefinitionSet = NULL,
    databaseId = NULL,
    databaseName = NULL,
    databaseDescription = NULL,
    connectionDetails = NULL,
    connection = NULL,
    cdmDatabaseSchema = NULL,
    vocabularyDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = NULL,
    cohortTable = "cohort",
    vocabularyVersionCdm = NULL,
    vocabularyVersion = NULL
) {
  #
  # Check parameters
  #

  exportFolder |> checkmate::assertDirectoryExists()
  cohortIdCases |> checkmate::assertNumeric()
  cohortIdControls |> checkmate::assertNumeric()
  analysisIds |> checkmate::assertNumeric()
  temporalStartDays |> checkmate::assertNumeric()
  temporalEndDays |> checkmate::assertNumeric()
  minCellCount |> checkmate::assertNumeric()


  if (is.null(cohortTableHandler) & any(is.null(c(cohortDefinitionSet, databaseId, databaseName, databaseDescription, connectionDetails, connection, cdmDatabaseSchema, cohortDatabaseSchema, vocabularyDatabaseSchema, vocabularyVersionCdm, vocabularyVersion)))) {
    stop("You must provide either a CohortTableHandler object or the necessary parameters to create one.")
  }

  if (!is.null(cohortTableHandler)) {
    cohortTableHandler |> checkmate::assertR6(class = "CohortTableHandler")

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
  }


  #
  # function
  #
  covariateSettings  <- FeatureExtraction_createTemporalCovariateSettingsFromList(
    analysisIds = analysisIds,
    temporalStartDays = temporalStartDays,
    temporalEndDays = temporalEndDays
  )

  ParallelLogger::logInfo("Getting FeatureExtraction for cases and controls")
  covariateCasesControls <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTable,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortIds = c(cohortIdCases, cohortIdControls),
    aggregated = T
  )

  ParallelLogger::logInfo("calcualting number of subjects with observation case and controls in each time window")

  cohortTbl <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, cohortTable))
  observationPeriodTbl <- dplyr::tbl(connection, tmp_inDatabaseSchema(cdmDatabaseSchema, "observation_period"))

  # TEMP, when therea are more than one covariate settings, get the first (it should use timeWindow from server instead)
  # if covariateSettings doent have the temporalStartDays, it means it is a list of settings
  if (length(covariateSettings$temporalStartDays) == 0) {
    covariateSettings <- covariateSettings[[1]]
  }

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
      covariateCasesControls$covariates |> tibble::as_tibble() |>
        dplyr::filter(cohortDefinitionId == cohortIdCases) |>
        dplyr::select(covariateId, timeId, n_cases_yes=sumValue),
      covariateCasesControls$covariates |> tibble::as_tibble() |>
        dplyr::filter(cohortDefinitionId == cohortIdControls) |>
        dplyr::select( covariateId, timeId, n_controls_yes=sumValue),
      by = c("covariateId", "timeId")
    )  |>
    dplyr::left_join(
      windowCounts,
      by="timeId"
    ) |>
    dplyr::mutate(
      n_cases_yes = dplyr::if_else(is.na(n_cases_yes), 0, n_cases_yes),
      n_controls_yes = dplyr::if_else(is.na(n_controls_yes), 0, n_controls_yes),
      n_cases = dplyr::if_else(is.na(n_cases), 0, n_cases),
      n_controls = dplyr::if_else(is.na(n_controls), 0, n_controls),
      # force n_cases to be same or lower than n_cases_yes, same for controls
      n_cases = dplyr::if_else(n_cases < n_cases_yes, n_cases_yes, n_cases),
      n_controls = dplyr::if_else(n_controls < n_controls_yes, n_controls_yes, n_controls),
      #
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
  cohortDefinitionSet |>
    dplyr::select(cohortId, cohortName, sql, json, subsetParent, isSubset, subsetDefinitionId) |>
    .writeToCsv(
      fileName = file.path(exportFolder, "cohort.csv")
    )

  # cohort counts ------------------------------------------------
  CohortGenerator::getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = c(cohortIdCases, cohortIdControls),
    databaseId = databaseId
  ) |>
   #rename to camelcase
    dplyr::rename_all(SqlRender::camelCaseToSnakeCase) |>
    .writeToCsv(
      fileName = file.path(exportFolder, "cohort_counts.csv")
    )

  # timeCodeWasCounts ------------------------------------------------
  timeCodeWasCounts |>
    dplyr::mutate(database_id = databaseId) |>
    dplyr::select(database_id, dplyr::everything()) |>
    .writeToCsv(
      fileName = file.path(exportFolder, "temporal_covariate_timecodewas.csv")
    )


  covariateCasesControls$covariateRef |> tibble::as_tibble() |>
    .writeToCsv(
      fileName = file.path(exportFolder, "temporal_covariate_ref.csv")
    )

  covariateCasesControls$analysisRef |> tibble::as_tibble() |>
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
    tablePrefix = "",
    analysis = "timeCodeWAS"
){

  if (file.exists(sqliteDbPath) & !overwrite) {
    stop("File ", sqliteDbPath, " already exists. Set overwrite = TRUE to replace")
  } else if (file.exists(sqliteDbPath)) {
    unlink(sqliteDbPath)
  }

  if (analysis == "timeCodeWAS") {
    specPath <- system.file("settings", "resultsDataModelSpecifications_TimeCodeWAS.csv", package = utils::packageName())
  }

  if (analysis == "codeWAS") {
    specPath <- system.file("settings", "resultsDataModelSpecifications_CodeWAS.csv", package = utils::packageName())
  }

  if (analysis == "cohortOverlaps") {
    specPath <- system.file("settings", "resultsDataModelSpecifications_CohortOverlaps.csv", package = utils::packageName())
  }

  if (analysis == "cohortDemographics") {
    specPath <- system.file("settings", "resultsDataModelSpecifications_CohortDemographics.csv", package = utils::packageName())
  }

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




























