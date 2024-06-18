
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
    covariatesIds = NULL,
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
  covariatesIds |> checkmate::assertNumeric(null.ok = TRUE)
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
  noCovariatesMode <- is.null(covariatesIds) || length(covariatesIds) == 0
  # if there is covariates we need to take all the events, if not we take the aggragate
  aggregated <- noCovariatesMode

  covariateSettings  <- FeatureExtraction_createTemporalCovariateSettingsFromList(
    analysisIds = analysisIds,
    temporalStartDays = -99999,
    temporalEndDays = 99999
  )

  ParallelLogger::logInfo("Getting FeatureExtraction for cases, aggregated=", aggregated)
  covariate_case <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTable,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortIds = cohortIdCases,
    aggregated = aggregated
  )
  ParallelLogger::logInfo("Getting FeatureExtraction for controls aggregated=", aggregated)
  covariate_control <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTable,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortIds = cohortIdControls,
    aggregated = aggregated
  )

  cohortCounts <- cohortTableHandler$getCohortCounts()
  n_cases_total <- cohortCounts$cohortSubjects[cohortCounts$cohortId == cohortIdCases]
  n_controls_total <- cohortCounts$cohortSubjects[cohortCounts$cohortId == cohortIdControls]


  #
  # noCovariatesMode
  #
  if (noCovariatesMode){

    # binary
    covariateCountsBinary <- tibble::tibble()
    if (covariate_case$covariates |> dplyr::count()  |> dplyr::pull(n) != 0 & covariate_control$covariates |> dplyr::count()  |> dplyr::pull(n) != 0) {

      ParallelLogger::logInfo("Running Fisher's Exact Test for binary covariates")

      covariateCases <- covariate_case$covariates  |>
        dplyr::select(covariateId, n_cases_yes = sumValue)  |> dplyr::collect()

      covariateControls <- covariate_control$covariates  |>
        dplyr::select(covariateId, n_controls_yes = sumValue)  |> dplyr::collect()

      covariateCountsBinary <- dplyr::full_join(covariateCases, covariateControls, by = "covariateId")  |>
        dplyr::mutate(
          n_cases_yes = ifelse(is.na(n_cases_yes), 0, n_cases_yes),
          n_controls_yes = ifelse(is.na(n_controls_yes), 0, n_controls_yes),
          n_cases_total = {{n_cases_total}},
          n_controls_total = {{n_controls_total}},
          n_cases_no = n_cases_total - n_cases_yes,
          n_controls_no = n_controls_total - n_controls_yes
        )

      covariateCountsBinary <- .addFisherTestToCodeCounts(covariateCountsBinary) |>
        dplyr::transmute(
          covariate_id = covariateId,
          covariate_type = "binary",
          n_cases_yes = n_cases_yes,
          mean_cases = n_cases_yes / n_cases_total,
          sd_cases = sqrt(mean_cases * (1 - mean_cases)),
          n_controls_yes = n_controls_yes,
          mean_controls = n_controls_yes / n_controls_total,
          sd_controls = sqrt(mean_controls * (1 - mean_controls)),
          p_value = p_value,
          odds_ratio = dplyr::if_else(is.infinite(odds_ratio), .Machine$double.xmax, odds_ratio),
          beta = NA_real_,
          standard_error = NA_real_,
          model_type = "Fisher's Exact Test",
          run_notes =  ''
        )
    }

    # continuous
    covariateCountsContinuous <- tibble::tibble()
    if (covariate_case$covariatesContinuous |> dplyr::count()  |> dplyr::pull(n) != 0 & covariate_control$covariatesContinuous |> dplyr::count()  |> dplyr::pull(n) != 0) {

      ParallelLogger::logInfo("Running Welch's t-test for continuous covariates")

      covariateCases <- covariate_case$covariatesContinuous  |>
        dplyr::select(covariateId, n_cases_yes = countValue, mean_cases = averageValue, sd_cases = standardDeviation)  |>
        dplyr::collect()

      covariateControls <- covariate_control$covariatesContinuous  |>
        dplyr::select(covariateId, n_controls_yes = countValue, mean_controls = averageValue, sd_controls = standardDeviation)  |>
        dplyr::collect()

      covariateCountsContinuous <- dplyr::full_join(covariateCases, covariateControls, by = "covariateId")

      covariateCountsContinuous <- .addTTestToCodeCounts(covariateCountsContinuous) |>
        dplyr::transmute(
          covariate_id = covariateId,
          covariate_type = "continuous",
          n_cases_yes = n_cases_yes,
          mean_cases = mean_cases,
          sd_cases = sd_cases,
          n_controls_yes = n_controls_yes,
          mean_controls = mean_controls,
          sd_controls = sd_controls,
          p_value = p_value,
          odds_ratio = NA_real_,
          beta = NA_real_,
          standard_error = SE,
          model_type = "Welch Two Sample t-test",
          run_notes = ""
        )
    }
    codewasResults <- dplyr::bind_rows(covariateCountsBinary, covariateCountsContinuous)

  }else{
    #
    # CovariatesMode
    #
    ParallelLogger::logInfo("Running regresions with covariates")

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
      stop("Cases dont have any covariates to compare")
    }
    # error if no patients in controls
    if (nrow(covarControl) == 0) {
      stop("Controls dont have any covariates to compare")
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

    codewasResults  <- tibble::tibble()

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
        dplyr::mutate(dplyr::across(BinaryCovariateNames, ~dplyr::if_else(is.na(.), FALSE, TRUE) )) |>
        as.data.frame()

      # calculate using speedglm
      outcomes  <- covariatesWideTable  |> select(3:ncol(covariatesWideTable))  |> colnames()
      predictors <- covariatesWideTable  |> select(2)  |> colnames()
      covariates  <- as.character(covariatesIds)
      outcomes <- outcomes |> dplyr::setdiff(covariates)

      ParallelLogger::logInfo("Running ", length(outcomes), " regresions for ", nrow(covariatesWideTable), " subjects, and ", length(covariates), " covariates")
      ParallelLogger::logInfo("Using ", cores, " cores")

      cluster <- ParallelLogger::makeCluster(numberOfThreads = cores)

      # x
      if (cores == 1){
        x  <- list(outcomes)
      }else{
        x <- split(outcomes, cut(seq_along(outcomes), breaks=cores, labels=FALSE))
      }

      # fun
      .fun <- function(outcomes, predictors, covariates, data){

        results <- tibble::tibble()
        for (outcome in outcomes){

          formula  <-  as.formula( paste( paste0('`',outcome,'`'), ' ~ ', paste(paste0('`',c(predictors, covariates),'`'), collapse = ' + ')) )
          family  <- if(data[[outcome]] |> is.logical()){binomial()}else{gaussian()}
          model <- NULL
          tryCatch({
            model = speedglm::speedglm(formula, data, family)
          }, error = function(e){
            error =  paste0("[Error in speedglm: ", e$message, "]")
          })

          #If the models did not converge or error, report NA values instead.
          or=NA; beta=NA; se=NA; p=NA; note=""
          if ( is.null(model) ){
            note = error
          } else {
            if(model$convergence) {
              gen_list=grep(predictors,row.names(modsum$coefficients))
              or=exp(modsum$coefficients[gen_list,1])
              beta=modsum$coefficients[gen_list,1]
              se=modsum$coefficients[gen_list,2]
              p=modsum$coefficients[gen_list,4]
            } else {
              note=paste(note,"[Error: The model did not converge]")
            }

          }

          else if (model$converged



             && model$convergence) {
            gen_list=grep(predictors,row.names(modsum$coefficients))
            or=exp(modsum$coefficients[gen_list,1])
            beta=modsum$coefficients[gen_list,1]
            se=modsum$coefficients[gen_list,2]
            p=modsum$coefficients[gen_list,4]
          } else {
            note=paste(note,"[Error: The model did not converge]")
          }

          if (family$family == "binomial") {
            # n is the number of TRUE
            covariate_type = "binary"
            n_cases_yes = sum(data[[outcome]][data$caseControl == TRUE], na.rm = TRUE)
            n_controls_yes = sum(data[[outcome]][data$caseControl == FALSE], na.rm = TRUE)
            model_type = "Logistic regresion"
          }else{
            # n is the number of no na
            covariate_type = "continuous"
            n_cases_yes = sum(!is.na(data[[outcome]][data$caseControl == TRUE]))
            n_controls_yes = sum(!is.na(data[[outcome]][data$caseControl == FALSE]))
            model_type = "Linear Regresion"
          }

          results <- dplyr::bind_rows(results, tibble::tibble(
            covariate_id = outcome,
            covariate_type = covariate_type,
            n_cases_yes = n_cases_yes,
            mean_cases = mean(data[[outcome]][data$caseControl == TRUE], na.rm = TRUE),
            sd_cases = sd(data[[outcome]][data$caseControl == TRUE], na.rm = TRUE),
            n_controls_yes = n_controls_yes,
            mean_controls = mean(data[[outcome]][data$caseControl == FALSE], na.rm = TRUE),
            sd_controls = sd(data[[outcome]][data$caseControl == FALSE], na.rm = TRUE),
            p_value = p,
            odds_ratio = or,
            beta = beta,
            standard_error = se,
            model_type = model_type,
            run_notes = note
          ))
        }
        return(results)
      }

      parallelResults <- ParallelLogger::clusterApply(cluster, x, .fun, predictors, covariates, covariatesWideTable)
      chunkResults <- dplyr::bind_rows(parallelResults)
      ParallelLogger::stopCluster(cluster)

      codewasResults <- dplyr::bind_rows(codewasResults, chunkResults)
    }
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
  cohortCounts |>
    dplyr::mutate(
      cohort_id = cohortId,
      cohort_entries = cohortEntries,
      cohort_subjects = cohortSubjects,
      database_id = databaseId
    ) |> .writeToCsv(
      fileName = file.path(exportFolder, "cohort_counts.csv")
    )

  # CodeWASCounts ------------------------------------------------
  codewasResults |>
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



.addTTestToCodeCounts <- function(CodeCounts){

  checkmate::assertDataFrame(CodeCounts)
  CodeCounts |> names() |> checkmate::testSubset(c("n_cases_yes", "mean_cases", "sd_cases", "n_controls_yes", "mean_controls", "sd_controls"))

  CodeCounts <- CodeCounts |>
    dplyr::bind_cols(
      CodeCounts |>
        dplyr::select(mean_cases,mean_controls,sd_cases,sd_controls,n_cases_yes,n_controls_yes) |>
        purrr::pmap_df( ~.t.test (..1,..2,..3,..4,..5,..6))
    )

  return(CodeCounts)

}


# m1, m2: the sample means
# s1, s2: the sample standard deviations
# n1, n2: the same sizes
# m0: the null value for the difference in means to be tested for. Default is 0.
# equal.variance: whether or not to assume equal variance. Default is FALSE.
.t.test <- function(m1,m2,s1,s2,n1,n2,m0=0,equal.variance=FALSE)
{
  if( equal.variance==FALSE )
  {
    se <- sqrt( (s1^2/n1) + (s2^2/n2) )
    # welch-satterthwaite df
    df <- ( (s1^2/n1 + s2^2/n2)^2 )/( (s1^2/n1)^2/(n1-1) + (s2^2/n2)^2/(n2-1) )
  } else
  {
    # pooled standard deviation, scaled by the sample sizes
    se <- sqrt( (1/n1 + 1/n2) * ((n1-1)*s1^2 + (n2-1)*s2^2)/(n1+n2-2) )
    df <- n1+n2-2
  }
  t <- (m1-m2-m0)/se
  #dat <- c(m1-m2, se, t, 2*pt(-abs(t),df))
  #names(dat) <- c("Difference of means", "Std Error", "t", "p-value")
  return(list(
    p_value = 2*pt(-abs(t),df),
    SE = se
  ))
}

.fixInf <- function(x){
  if (is.infinite(x)){
    return(NA)
  }else{
    return(x)
  }
}



