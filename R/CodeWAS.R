
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
  cohortIdCases |> checkmate::assertNumeric()
  cohortIdControls |> checkmate::assertNumeric()
  analysisIds |> checkmate::assertNumeric()
  checkmate::assertSubset(analysisIds, getListOfAnalysis()$analysisId)
  covariatesIds |> checkmate::assertNumeric()

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
    temporalStartDays = 99999,
    temporalEndDays = -99999
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


  covarCase  <- covariate_case$covariates |>
    dplyr::left_join(covariate_case$covariateRef, by = "covariateId") |>
    dplyr::left_join(covariate_case$analysisRef, by = "analysisId")  |>
    dplyr::transmute(
      personId = rowId,
      covariateId = covariateId,
      covariateValue = covariateValue,
      isBinary = isBinary
    ) |> tibble::as_tibble()

  covarControl  <- covariate_control$covariates |>
    dplyr::left_join(covariate_control$covariateRef, by = "covariateId") |>
    dplyr::left_join(covariate_control$analysisRef, by = "analysisId")  |>
    dplyr::transmute(
      personId = rowId,
      covariateId = covariateId,
      covariateValue = covariateValue,
      isBinary = isBinary
    ) |>
    tibble::as_tibble() |>
    dplyr::anti_join(covarCase, by = "personId")

  # case control covariates
  caseControlCovar <- dplyr::bind_rows(
    covarCase |>dplyr::mutate(personId=personId, covariateId="caseControl", covariateValue=TRUE) ,
    covarControl |> dplyr::mutate(personId=personId, covariateId="caseControl", covariateValue=FALSE)
  )  |>
    dplyr::select(-isBinary) |>
    dplyr::distinct() |>
    tidyr::spread(covariateId, covariateValue)


  # binary covariates
  binaryCovar <- dplyr::bind_rows(
    covarCase ,
    covarControl
  )  |>
    dplyr::filter(isBinary=='Y') |>
    dplyr::select(personId, covariateId, covariateValue) |>
    dplyr::distinct() |>
    dplyr::mutate(covariateValue = ifelse(covariateValue == 1, TRUE, FALSE)) |>
    tidyr::spread(covariateId, covariateValue) |>
    dplyr::mutate(dplyr::across(dplyr::everything(), ~dplyr::if_else(is.na(.), FALSE, .) ))


  # continuous covariates
  continuousCovar <- dplyr::bind_rows(
    covarCase ,
    covarControl
  )  |>
    dplyr::filter(isBinary!='Y') |>
    dplyr::select(personId, covariateId, covariateValue) |>
    dplyr::distinct() |>
    tidyr::spread(covariateId, covariateValue)


 # calculate codewas using PheWAS::phewas
  covariatesWideTable <- caseControlCovar |>
    dplyr::left_join(binaryCovar, by = "personId") |>
    dplyr::left_join(continuousCovar, by = "personId")


  outcomes  <- covariatesWideTable  |> select(3:ncol(covariatesWideTable))  |> colnames()
  predictors <- covariatesWideTable  |> select(2)  |> colnames()
  covariates  <- as.character(covariatesIds)
  outcomes <- outcomes |> dplyr::setdiff(covariates)


  phewasResults <- PheWAS::phewas(
    outcomes = outcomes,
    predictors = predictors,
    covariates = covariates,
    data = covariatesWideTable |> as.data.frame(),
    additive.genotypes = FALSE,
    min.records = 0
  ) |>  tibble::as_tibble()




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


  # tmp tables ------------------------------------------------
  covariatesWideTable |>
    dplyr::transmute(idd = personId, snp = dplyr::if_else(caseControl, 2L, 0L)) |>
    dplyr::distinct() |>
    .writeToCsv(
      fileName = file.path(exportFolder, "tmp_id_snp.csv")
    )

  dplyr::bind_rows(
    covarCase ,
    covarControl
  ) |>
    dplyr::select(idd = personId, code = covariateId) |>
    dplyr::distinct() |>
    .writeToCsv(
      fileName = file.path(exportFolder, "tmp_id_code.csv")
    )



  ParallelLogger::logInfo("Results exported")
  return(TRUE)

}

