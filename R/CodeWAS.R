
#' CohortDiagnostics_runCodeWAS
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

executeCodeWAS <- function(
    exportFolder,
    cohortTableHandler = NULL,
    cohortIdCases,
    cohortIdControls,
    analysisIds,
    covariatesIds,
    minCellCount = 1,
    chunksSizeNOutcomes = NULL,
    cores = 1
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
  cohortIdCases |> checkmate::assertNumeric()
  cohortIdControls |> checkmate::assertNumeric()
  analysisIds |> checkmate::assertNumeric()
  checkmate::assertSubset(analysisIds, getListOfAnalysis()$analysisId)
  covariatesIds |> checkmate::assertNumeric()
  minCellCount |> checkmate::assertNumeric()
  if (is.null(chunksSizeNOutcomes)) {
    chunksSizeNOutcomes <- 1000000000000
  }
  chunksSizeNOutcomes |> checkmate::assertNumeric()
  cores |> checkmate::assertNumeric()

  covariatesNoAnalysis  <- setdiff(covariatesIds %% 1000, analysisIds)
  if (length(covariatesNoAnalysis) > 0) {
    stop("The following covariates do not have an associated analysis: ", covariatesNoAnalysis)
  }


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

  covariateSettings  <- FeatureExtraction_createTemporalCovariateSettingsFromList(
    analysisIds = analysisIds,
    temporalStartDays = -99999,
    temporalEndDays = 99999
    )

  ParallelLogger::logInfo("Getting FeatureExtraction for cases")
  covariate_case <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTable,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortIds = cohortIdCases,
    aggregated = F
  )
  ParallelLogger::logInfo("Getting FeatureExtraction for controls")
  covariate_control <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTable,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortIds = cohortIdControls,
    aggregated = F
  )

  # case control covariates
  covarCase <- covariate_case$covariates |>
    dplyr::distinct(rowId)  |>
    dplyr::collect() |>
    dplyr::transmute(personId=rowId, covariateId="caseControl", covariateValue=TRUE)

  covarControl <- covariate_control$covariates |>
    dplyr::distinct(rowId) |>
    dplyr::collect() |>
    dplyr::transmute(personId=rowId, covariateId="caseControl", covariateValue=FALSE)

  # error if no patients in cases
  if (nrow(covarCase) == 0) {
    stop("Cases dont have any covarite to compare")
  }
  # error if no patients in controls
  if (nrow(covarControl) == 0) {
    stop("Controls dont have any covarite to compare")
  }

  # warning if patient ids overlap
  nOverlap <- length(intersect(covarCase$personId, covarControl$personId))
  if (nOverlap > 0) {
    warning("There are ", nOverlap, " patients that are in both cases and controls, cases were removed from controls.")
    covarControl <- covarControl |> dplyr::filter(!personId %in% covarCase$personId)
  }

  caseControlCovariateWide <- dplyr::bind_rows(covarCase,covarControl)  |>
    tidyr::spread(covariateId, covariateValue)


  #
  # break analysis into chunks of covariates
  #
  allCovariates <- dplyr::bind_rows(
    covariate_case$covariates |>
      dplyr::left_join(covariate_case$covariateRef, by = "covariateId") |>
      dplyr::left_join(covariate_case$analysisRef, by = "analysisId")  |>
      dplyr::distinct(covariateId, isBinary) |>
      dplyr::collect(),
    covariate_control$covariates |>
      dplyr::left_join(covariate_control$covariateRef, by = "covariateId") |>
      dplyr::left_join(covariate_control$analysisRef, by = "analysisId")  |>
      dplyr::distinct(covariateId, isBinary) |>
      dplyr::collect()
  ) |> dplyr::distinct()

  phewasResults  <- NULL

  ParallelLogger::logInfo("Running ", nrow(allCovariates), " covariates in ", length(seq(1, nrow(allCovariates), chunksSizeNOutcomes)), " chunks of ", chunksSizeNOutcomes, " covariates")
  for (i in seq(1, nrow(allCovariates), chunksSizeNOutcomes)) {
    ParallelLogger::logInfo("Running chunk ", i, " to ", min(i + chunksSizeNOutcomes - 1, nrow(allCovariates)) )
    covariateIdsChunk <- allCovariates  |> dplyr::slice(i:min(i + chunksSizeNOutcomes - 1, nrow(allCovariates)))  |> dplyr::pull(covariateId)
    # add controling covariates
    covariateIdsChunk <- union(covariateIdsChunk, covariatesIds)

    BinaryCovariateNames <- covariateIdsChunk |> intersect(allCovariates$covariateId[allCovariates$isBinary=='Y']) |> as.character()

    ParallelLogger::logInfo("Prepare data for ", length(covariateIdsChunk), " covariates")

    # covariates
    covariatesWideTable <- dplyr::bind_rows(
      covariate_case$covariates |>
        dplyr::filter(covariateId %in% covariateIdsChunk) |>
        dplyr::distinct(rowId, covariateId, covariateValue) |>
        dplyr::collect(),
      covariate_control$covariates |>
        dplyr::filter(covariateId %in% covariateIdsChunk) |>
        dplyr::distinct(rowId, covariateId, covariateValue) |>
        dplyr::collect() |>
          dplyr::anti_join(covarCase, by = c("rowId" = "personId"))
    )  |>
      dplyr::distinct() |>
      tidyr::spread(covariateId, covariateValue)

    covariatesWideTable <- caseControlCovariateWide |>
      dplyr::left_join(covariatesWideTable, by = c("personId"="rowId")) |>
      dplyr::mutate(dplyr::across(BinaryCovariateNames, ~dplyr::if_else(is.na(.), FALSE, TRUE) ))

    # calculate phewas using PheWAS::phewas
    outcomes  <- covariatesWideTable  |> select(3:ncol(covariatesWideTable))  |> colnames()
    predictors <- covariatesWideTable  |> select(2)  |> colnames()
    covariates  <- as.character(covariatesIds)
    outcomes <- outcomes |> dplyr::setdiff(covariates)

    ParallelLogger::logInfo("Running ", length(outcomes), " regresions for ", nrow(covariatesWideTable), " subjects, and ", length(covariates), " covariates")
    ParallelLogger::logInfo("Using ", cores, " cores")

    res <- PheWAS::phewas(
      outcomes = outcomes,
      predictors = predictors,
      covariates = NA,
      data = covariatesWideTable |> as.data.frame(),
      additive.genotypes = FALSE,
      min.records = 0,
      cores = 1,
      unadjusted = TRUE,
      adjustments = NA
    ) |>  tibble::as_tibble()

    phewasResults <- dplyr::bind_rows(phewasResults, res)

  }


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

  # CodeWASCounts ------------------------------------------------
  phewasResults |>
    dplyr::transmute(
      covariate_id = outcome,
      n_total = n_total,
      n_cases = n_cases,
      n_controls = n_controls,
      p_value = p,
      odds_ratio = OR,
      beta = beta,
      standard_error = SE,
      model_type = type,
      run_notes = note
    ) |>
    dplyr::mutate(database_id = databaseId) |>
    dplyr::select(database_id, dplyr::everything()) |>
    .writeToCsv(
      fileName = file.path(exportFolder, "codewas_results.csv")
    )

  dplyr::bind_rows(
    covariate_case$analysisRef |> tibble::as_tibble(),
    covariate_control$analysisRef |> tibble::as_tibble()
  ) |>
    dplyr::distinct() |>
    .writeToCsv(
      fileName = file.path(exportFolder, "analysis_ref.csv")
    )

  dplyr::bind_rows(
    covariate_case$covariateRef |> tibble::as_tibble(),
    covariate_control$covariateRef |> tibble::as_tibble()
  ) |>
    dplyr::distinct() |>
    .writeToCsv(
      fileName = file.path(exportFolder, "covariate_ref.csv")
    )

  ParallelLogger::logInfo("Results exported")
  return(TRUE)

}

