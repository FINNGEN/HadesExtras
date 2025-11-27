addStatisticalTestsToCovariatesAndromeda <- function(
  covariatesAndromeda,
  comparisonsTible,
  analysisTypes = c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous"),
  nChunks = NULL
) {
    if (is.null(nChunks)) {
        nChunks <- future::availableCores() - 1
    }
    nChunks |> checkmate::assertNumber(upper = future::availableCores() - 1)

    covariatesAndromeda |> checkmate::assertClass("Andromeda")
    comparisonsTible |>
        names() |>
        checkmate::assertSubset(c("comparisonId", "caseCohortId", "controlCohortId"))

    covariatesAndromeda$comparisons <- comparisonsTible

    covariatesAndromeda$statisticalTests <- tibble::tibble(
        comparisonId = integer(),
        analysisId = integer(),
        unit = character(),
        caseCohortId = integer(),
        controlCohortId = integer(),
        conceptId = integer(),
        pValue = numeric(),
        effectSize = numeric(),
        standarizeMeanDifference = numeric(),
        testName = character()
    )
    
    if ("Binary" %in% analysisTypes) {
        ParallelLogger::logInfo("Adding statistical tests to binary covariates")
        
        analysisCaseControl <- covariatesAndromeda$analysisRef |>
            dplyr::filter(analysisType == "Binary") |>
            dplyr::select(analysisId) |>
            dplyr::cross_join(covariatesAndromeda$comparisons)

        binaryCovariatesTible <- dplyr::full_join(
            # Case
            analysisCaseControl |>
                dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
                dplyr::inner_join(covariatesAndromeda$covariates, by = c("analysisId" = "analysisId", "caseCohortId" = "cohortDefinitionId")) |>
                dplyr::left_join(covariatesAndromeda$cohortCounts, by = c("caseCohortId" = "cohortId"))  ,
            # Control
            analysisCaseControl |>
                dplyr::select(comparisonId, analysisId, caseCohortId,  controlCohortId ) |>
                dplyr::inner_join(covariatesAndromeda$covariates, by = c("analysisId" = "analysisId", "controlCohortId" = "cohortDefinitionId")) |>
                dplyr::left_join(covariatesAndromeda$cohortCounts, by = c("controlCohortId" = "cohortId")),
            #
            by = c("comparisonId", "analysisId", "conceptId", "caseCohortId", "controlCohortId")
        ) |>
            dplyr::transmute(
                comparisonId = comparisonId,
                analysisId = analysisId,
                caseCohortId = caseCohortId,
                controlCohortId = controlCohortId,
                conceptId = conceptId,
                sumValue.x = dplyr::if_else(is.na(sumValue.x), 0, sumValue.x),
                sumValue.y = dplyr::if_else(is.na(sumValue.y), 0, sumValue.y), 
                cohortSubjects.x = cohortSubjects.x,
                cohortSubjects.y = cohortSubjects.y
            ) |>
            dplyr::collect() |>
            dplyr::group_by(comparisonId, analysisId, caseCohortId, controlCohortId, conceptId) |>
            dplyr::summarize(
                contingencyTable = purrr::pmap(
                    .l = list(sumValue.x, cohortSubjects.x - sumValue.x, sumValue.y, cohortSubjects.y - sumValue.y),
                    .f = function(nCasesYes, nCasesNo, nControlsYes, nControlsNo) {
                        mat <- matrix(c(nCasesYes, nCasesNo, nControlsYes, nControlsNo), nrow = 2, byrow = TRUE)
                        colnames(mat) <- c("Yes", "No")
                        rownames(mat) <- c("cases", "controls")
                        mat
                    }
                ),
                .groups = "drop"
            )

        binaryCovariatesTible <- .addStatisticalTestsParallel(binaryCovariatesTible, "BinaryTest", nChunks)
        binaryCovariatesTible <- binaryCovariatesTible |>
            dplyr::mutate(unit = NA_character_) |>
            dplyr::select(comparisonId, analysisId, unit, caseCohortId, controlCohortId, conceptId, pValue, effectSize, standarizeMeanDifference, testName)

        Andromeda::appendToTable(covariatesAndromeda$statisticalTests, binaryCovariatesTible)
        rm(binaryCovariatesTible)
    }

    if ("Categorical" %in% analysisTypes) {
        ParallelLogger::logInfo("Adding statistical tests to categorical covariates")
        analysisCaseControl <- covariatesAndromeda$analysisRef |>
            dplyr::filter(analysisType == "Categorical") |>
            dplyr::select(analysisId) |>
            dplyr::cross_join(covariatesAndromeda$comparisons)

        categoricalCovariatesTible <- dplyr::full_join(
            # Case
            analysisCaseControl |>
                dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
                dplyr::inner_join(covariatesAndromeda$covariates, by = c("analysisId" = "analysisId", "caseCohortId" = "cohortDefinitionId")),
            # Control
            analysisCaseControl |>
                dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
                dplyr::inner_join(covariatesAndromeda$covariates, by = c("analysisId" = "analysisId", "controlCohortId" = "cohortDefinitionId")),
            #
            by = c("comparisonId", "analysisId", "conceptId", "categoryId", "caseCohortId", "controlCohortId")
        ) |>
            dplyr::transmute(
                comparisonId = comparisonId,
                analysisId = analysisId,
                caseCohortId = caseCohortId,
                controlCohortId = controlCohortId,
                conceptId = conceptId,
                categoryId = categoryId,
                sumValue.x = dplyr::if_else(is.na(sumValue.x), 0, sumValue.x),
                sumValue.y = dplyr::if_else(is.na(sumValue.y), 0, sumValue.y)
            ) |>
            dplyr::collect() |>
            dplyr::group_by(comparisonId, analysisId, caseCohortId, controlCohortId, conceptId) |>
            dplyr::summarize(
                contingencyTable = list({
                    mat <- matrix(c(sumValue.x, sumValue.y), nrow = 2, byrow = TRUE)
                    colnames(mat) <- as.character(categoryId)
                    rownames(mat) <- c("cases", "controls")
                    if (ncol(mat) == 1) {
                        mat <- NULL
                    }
                    mat
                }),
                .groups = "drop"
            ) |>
            dplyr::filter(!purrr::map_lgl(contingencyTable, ~ is.null(.)))

        categoricalCovariatesTible <- .addStatisticalTestsParallel(categoricalCovariatesTible, "CategoricalTest", nChunks)
        categoricalCovariatesTible <- categoricalCovariatesTible |>
            dplyr::mutate(unit = NA_character_) |>
            dplyr::select(comparisonId, analysisId, unit, caseCohortId, controlCohortId, conceptId, pValue, effectSize, standarizeMeanDifference, testName)

        Andromeda::appendToTable(covariatesAndromeda$statisticalTests, categoricalCovariatesTible)
        rm(categoricalCovariatesTible)
    }
    
    if ("Counts" %in% analysisTypes) {
        ParallelLogger::logInfo("Adding statistical tests to counts covariates")
        analysisCaseControl <- covariatesAndromeda$analysisRef |>
            dplyr::filter(analysisType == "Counts") |>
            dplyr::select(analysisId) |>
            dplyr::cross_join(covariatesAndromeda$comparisons)

        countsCovariatesTible <- dplyr::full_join(
            # Case
            analysisCaseControl |>
                dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
                dplyr::inner_join(covariatesAndromeda$covariatesContinuous, by = c("analysisId" = "analysisId", "caseCohortId" = "cohortDefinitionId")),
            # Control
            analysisCaseControl |>
                dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
                dplyr::inner_join(covariatesAndromeda$covariatesContinuous, by = c("analysisId" = "analysisId", "controlCohortId" = "cohortDefinitionId")), 
            #
            by = c("comparisonId", "analysisId", "conceptId", "unit", "caseCohortId", "controlCohortId")
        ) |>
            dplyr::transmute(
                comparisonId = comparisonId,
                analysisId = analysisId,
                unit = unit,
                caseCohortId = caseCohortId,
                controlCohortId = controlCohortId,
                conceptId = conceptId,
                countValue.x = dplyr::if_else(is.na(countValue.x), 0, countValue.x),
                countValue.y = dplyr::if_else(is.na(countValue.y), 0, countValue.y),
                averageValue.x = averageValue.x,
                averageValue.y = averageValue.y,
                standardDeviation.x = standardDeviation.x,
                standardDeviation.y = standardDeviation.y
            ) |>
            dplyr::collect() 

        countsCovariatesTible <- .addStatisticalTestsParallel(countsCovariatesTible, "CountsTest", nChunks)
        countsCovariatesTible <- countsCovariatesTible |>
            dplyr::select(comparisonId, analysisId, unit, caseCohortId, controlCohortId, conceptId, pValue, effectSize, standarizeMeanDifference, testName)

        Andromeda::appendToTable(covariatesAndromeda$statisticalTests, countsCovariatesTible)
        rm(countsCovariatesTible)
    }

    if (any(c("Continuous", "AgeFirstEvent", "DaysToFirstEvent") %in% analysisTypes)) {
        ParallelLogger::logInfo("Adding statistical tests to continuous, age first event, and days to first event covariates")
        analysisCaseControl <- covariatesAndromeda$analysisRef |>
            dplyr::filter(analysisType %in% c("Continuous", "AgeFirstEvent", "DaysToFirstEvent")) |>
            dplyr::select(analysisId) |>
            dplyr::cross_join(covariatesAndromeda$comparisons)

        continuousCovariatesTible <- dplyr::full_join(
            # Case
            analysisCaseControl |>
                dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
                dplyr::inner_join(covariatesAndromeda$covariatesContinuous, by = c("analysisId" = "analysisId", "caseCohortId" = "cohortDefinitionId")),
            # Control
            analysisCaseControl |>
                dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
                dplyr::inner_join(covariatesAndromeda$covariatesContinuous, by = c("analysisId" = "analysisId", "controlCohortId" = "cohortDefinitionId")), 
            #
            by = c("comparisonId", "analysisId", "conceptId", "unit", "caseCohortId", "controlCohortId")
        ) |>
            dplyr::transmute(
                comparisonId = comparisonId,
                analysisId = analysisId,
                unit = unit,
                caseCohortId = caseCohortId,
                controlCohortId = controlCohortId,
                conceptId = conceptId,
                countValue.x = dplyr::if_else(is.na(countValue.x), 0, countValue.x),
                countValue.y = dplyr::if_else(is.na(countValue.y), 0, countValue.y),
                averageValue.x = averageValue.x,
                averageValue.y = averageValue.y,
                standardDeviation.x = standardDeviation.x,
                standardDeviation.y = standardDeviation.y
            ) |>
            dplyr::collect()

        continuousCovariatesTible <- .addStatisticalTestsParallel(continuousCovariatesTible, "ContinuousTest", nChunks)
        continuousCovariatesTible <- continuousCovariatesTible |>
            dplyr::select(comparisonId, analysisId, unit, caseCohortId, controlCohortId, conceptId, pValue, effectSize, standarizeMeanDifference, testName)

        Andromeda::appendToTable(covariatesAndromeda$statisticalTests, continuousCovariatesTible)
        rm(continuousCovariatesTible)
    }

    return(covariatesAndromeda)
}


#' @title parallelAddTestToCodeCounts
#' @description Runs .addTestToCodeCounts in parallel on chunks of the input tibble.
#' @param tibbleWithCodeCounts The tibble obtained from getCodeCounts.
#' @param n_chunks Number of chunks to split the tibble into (default: number of available cores).
#' @return Tibble with added p-value, odds ratio, and test columns.
#' @export
#' @importFrom dplyr bind_rows
#' @importFrom ParallelLogger makeCluster stopCluster clusterApply
.addStatisticalTestsParallel <- function(covariatesTible, analysisType, nChunks = max(1, parallel::detectCores() - 1), ...) {
    # Load required packages
    requireNamespace("ParallelLogger", quietly = TRUE)

    analysisType |> checkmate::assertChoice(c("BinaryTest", "CategoricalTest", "CountsTest", "ContinuousTest"))
    detectedCores <- parallel::detectCores()
    upperBound <- if (is.null(detectedCores) || is.na(detectedCores)) Inf else detectedCores
    nChunks |> checkmate::assertNumber(lower = 1, upper = upperBound)

    if (analysisType == "BinaryTest" || analysisType == "CategoricalTest") {
        covariatesTible |> checkmate::assertTibble()
        covariatesTible |>
            names() |>
            checkmate::testSubset(
                c("analysisId", "caseCohortId", "controlCohortId", "conceptId", "contingencyTable")
            )
        fun <- .addFisherOrChiSquareTest
    }
    if (analysisType == "CountsTest") {
        covariatesTible |> checkmate::assertTibble()
        covariatesTible |>
            names() |>
            checkmate::testSubset(
                c("analysisId", "caseCohortId", "controlCohortId", "conceptId", "meanValueCases", "sdValueCases", "nCasesYesWithValue", "meanValueControls", "sdValueControls", "nControlsYesWithValue")
            )
        fun <- .addNegativeBinomialTest
    }

    if (analysisType == "ContinuousTest") {
        covariatesTible |> checkmate::assertTibble()
        covariatesTible |>
            names() |>
            checkmate::testSubset(
                c("analysisId", "caseCohortId", "controlCohortId", "conceptId", "meanValueCases", "sdValueCases", "nCasesYesWithValue", "meanValueControls", "sdValueControls", "nControlsYesWithValue")
            )
        fun <- .addWelchTTest
    }

    # Split tibble into chunks / run sequentially when requested
    if (nChunks == 1L || nrow(covariatesTible) == 0) {
        result <- fun(covariatesTible, ...)
    } else {
        chunkCount <- min(nChunks, nrow(covariatesTible))
        chunks <- split(
            covariatesTible,
            cut(seq_len(nrow(covariatesTible)), breaks = chunkCount, labels = FALSE)
        )
        ParallelLogger::logInfo(paste("Running", analysisType, "tests in", chunkCount, "chunks of size", nrow(chunks[[1]])))
        cluster <- ParallelLogger::makeCluster(numberOfThreads = chunkCount)
        on.exit(ParallelLogger::stopCluster(cluster), add = TRUE)
        chunkResults <- ParallelLogger::clusterApply(cluster, chunks, fun, ...)
        result <- dplyr::bind_rows(chunkResults)
    }

    return(result)
}


#' @title .addFisherTestToCodeCounts
#' @description performs an Fisher test in each row of the case_controls_counts table
#' @param covariatesTible table obtained from `getCodeCounts` with contingencyTable column.
#' @return inputed table with appened columsn :
#' - p : for the p-value
#' - OR; for the ods ratio
#' @export
#' @importFrom dplyr setdiff bind_cols select mutate if_else
#' @importFrom purrr pmap_df
.addFisherOrChiSquareTest <- function(covariatesTible) {
    missing_collumns <- dplyr::setdiff(c("contingencyTable"), names(covariatesTible))
    if (length(missing_collumns) != 0) {
        stop("covariatesTible is missing the following columns: ", missing_collumns)
    }

    # Needs to be in the same function to be used in parallel
    .fisher <- function(contingencyTable) {
        # Extract the matrix from the list
        mat <- contingencyTable

        # Check if we have a valid contingency table
        if (is.null(mat) || ncol(mat) < 2 || nrow(mat) != 2) {
            return(list(
                pValue = NA_real_,
                effectSize = NA_real_,
                standarizeMeanDifference = NA_real_,
                testName = "Invalid contingency table"
            ))
        }

        # Determine if we should use Fisher's exact test or Chi-square
        # Check expected counts and sample sizes
        rowSums <- rowSums(mat)
        colSums <- colSums(mat)
        total <- sum(mat)
        expected <- outer(rowSums, colSums) / total
        useFisher <- any(expected < 5) || any(rowSums < 10) || any(colSums < 10) || any(mat < 10)

        # Perform the test (same logic for both 2x2 and 2xN tables)
        isWarn <- FALSE
        results <- NULL
        if (useFisher) {
            tryCatch(
                {
                    results <- stats::fisher.test(mat, workspace = 2e7)
                    p.value <- results$p.value
                    test <- "Fisher"
                },
                error = function(e) {
                    p.value <<- NA_real_
                    test <<- "Fisher (Error)"
                }
            )
        } else {
            tryCatch(
                {
                    results <- stats::chisq.test(mat)
                    p.value <- results$p.value
                    test <- "Chi-square"
                },
                warning = function(w) {
                    isWarn <<- TRUE
                },
                error = function(e) {
                    p.value <<- NA_real_
                    test <<- "Chi-square (Error)"
                }
            )
            # If chi-square gives warning, fall back to Fisher's
            if (isWarn) {
                tryCatch(
                    {
                        results <- stats::fisher.test(mat, workspace = 2e7)
                        p.value <- results$p.value
                        test <- "Fisher"
                    },
                    error = function(e) {
                        p.value <<- NA_real_
                        test <<- "Fisher (Error)"
                    }
                )
            }
        }

        # Calculate effect size based on table dimensions
        if (ncol(mat) == 2) {
            # For 2x2 tables: use odds ratio
            a <- mat[1, 1]
            b <- mat[1, 2]
            c <- mat[2, 1]
            d <- mat[2, 2]
            if (test == "Chi-square" && !isWarn) {
                effect.size <- (a * d) / (b * c)
            } else if (test == "Fisher" && !is.null(results)) {
                effect.size <- results$estimate
            } else {
                effect.size <- NA_real_
            }
            # Standardized mean difference for 2x2 tables
            # p1 = proportion of cases with condition, p2 = proportion of controls with condition
            p1 <- a / (a + b) # proportion of cases with yes
            p2 <- c / (c + d) # proportion of controls with yes
            # SMD = (p1 - p2) / sqrt((p1(1-p1) + p2(1-p2)) / 2)
            denominator <- sqrt((p1 * (1 - p1) + p2 * (1 - p2)) / 2)
            if (is.na(denominator) || denominator == 0) {
                h <- .Machine$double.xmax
            } else {
                h <- (p1 - p2) / denominator
            }
        } else {
            # For multi-category tables: use Cram<U+00E9>r's V
            # Calculate Cram<U+00E9>r's V regardless of which test was used
            if (!is.null(results) && test %in% c("Chi-square", "Fisher")) {
                # Calculate chi-square statistic from the table
                if (test == "Chi-square" && !isWarn) {
                    chi_sq <- results$statistic
                } else {
                    # For Fisher's test or when chi-square had warnings, calculate chi-square manually
                    rowSums <- rowSums(mat)
                    colSums <- colSums(mat)
                    total <- sum(mat)
                    expected <- outer(rowSums, colSums) / total
                    chi_sq <- sum((mat - expected)^2 / expected)
                }
                n <- sum(mat)
                min_dim <- min(nrow(mat), ncol(mat))
                cramers_v <- sqrt(chi_sq / (n * (min_dim - 1)))
                effect.size <- as.numeric(cramers_v)
                h <- effect.size
            } else {
                effect.size <- NA_real_
                h <- NA_real_
            }
        }

        return(list(
            pValue = p.value,
            effectSize = effect.size,
            standarizeMeanDifference = h,
            testName = test
        ))
    }

    covariatesTible <- covariatesTible |>
        dplyr::bind_cols(
            covariatesTible |>
                dplyr::select(contingencyTable) |>
                purrr::pmap_df(~ .fisher(..1))
        )

    return(covariatesTible)
}


#' @title Add statistical tests to covariates Andromeda object
#'
#' @description
#' Adds statistical test results (p-value, effect size, standardized mean difference, and test name)
#' to covariates generated and stored in an Andromeda object. The function handles different analysis
#' types (binary, categorical, counts, continuous, age-at-event, days-to-event) and applies appropriate
#' statistical tests for each type.
#'
#' @param covariatesAndromeda An Andromeda object containing covariate data and results from feature extraction.
#' @param caseControlTible A tibble with columns defining the case and control cohort IDs.
#' @param analysisTypes A character vector indicating the analysis types to compute statistical tests for.
#'
#' @return The updated Andromeda object with an added table of statistical test results.
#'
#' @importFrom dplyr bind_cols select setdiff
#' @importFrom purrr pmap_df
#' @importFrom stats fisher.test chisq.test pnorm
#' @importFrom tibble tibble
#'
#'
.addNegativeBinomialTest <- function(countsCovariatesTible) {
    missing_collumns <- dplyr::setdiff(c("averageValue.x", "standardDeviation.x", "countValue.x", "averageValue.y", "standardDeviation.y", "countValue.y"), names(countsCovariatesTible))
    if (length(missing_collumns) != 0) {
        stop("countsCovariatesTible is missing the following columns: ", missing_collumns)
    }

    .negativeBinomial <- function(mu1, sd1, n1, mu2, sd2, n2) {
        # special case
        if (n1 <= 2 | n2 <= 2) {
            return(list(
                pValue = NA_real_,
                effectSize = NA_real_,
                standarizeMeanDifference = NA_real_,
                testName = "Negative Binomial (Error: Less than 2 samples)"
            ))
        }

        # Step 1: dispersion
        k1 <- mu1^2 / (sd1^2 - mu1)
        k2 <- mu2^2 / (sd2^2 - mu2)

        # Step 2: variance of means
        var_mean1 <- (mu1 + mu1^2 / k1) / n1
        var_mean2 <- (mu2 + mu2^2 / k2) / n2
        SE <- sqrt(var_mean1 + var_mean2)

        # Step 3: Z-test
        Z <- (mu1 - mu2) / SE
        p <- 2 * (1 - pnorm(abs(Z)))

        # Step 4: rate ratio and CI
        IRR <- mu1 / mu2
        SE_logIRR <- sqrt(var_mean1 / mu1^2 + var_mean2 / mu2^2)
        # CI_low <- exp(log(RR) - 1.96 * SE_logRR)
        # CI_high <- exp(log(RR) + 1.96 * SE_logRR)

        smd <- log(IRR) / SE_logIRR

        return(list(
            pValue = p,
            effectSize = IRR,
            standarizeMeanDifference = smd,
            testName = "Negative Binomial"
        ))
    }

    countsCovariatesTible <- countsCovariatesTible |>
        dplyr::bind_cols(
            countsCovariatesTible |>
                dplyr::select(averageValue.x, standardDeviation.x, countValue.x, averageValue.y, standardDeviation.y, countValue.y) |>
                purrr::pmap_df(~ .negativeBinomial(..1, ..2, ..3, ..4, ..5, ..6))
        )

    return(countsCovariatesTible)
}


#' @title .addWelchTTest
#' @description Applies Welch's two-sample t-test to each row of a continuous
#' covariate tibble and appends the resulting statistics (p-value, raw effect
#' size, standardized mean difference, and test name).
#' @param continuousCovariatesTible Tibble with continuous covariate summaries
#' that must include mean and standard deviation per group as well as the number
#' of available observations (`meanValueCases`, `sdValueCases`,
#' `nCasesYesWithValue`, `meanValueControls`, `sdValueControls`,
#' `nControlsYesWithValue`).
#' @return The input tibble with four new columns: `pValue`, `effectSize`,
#' `standarizeMeanDifference`, and `testName`.
#' @importFrom dplyr bind_cols select
#' @importFrom purrr pmap_df
#'
.addWelchTTest <- function(
  continuousCovariatesTible
) {
    missing_collumns <- dplyr::setdiff(c("averageValue.x", "standardDeviation.x", "countValue.x", "averageValue.y", "standardDeviation.y", "countValue.y"), names(continuousCovariatesTible))
    if (length(missing_collumns) != 0) {
        stop("continuousCovariatesTible is missing the following columns: ", missing_collumns)
    }


    # m1, m2: the sample means
    # s1, s2: the sample standard deviations
    # n1, n2: the same sizes
    # m0: the null value for the difference in means to be tested for. Default is 0.
    # equal.variance: whether or not to assume equal variance. Default is FALSE.
    .welchTTest <- function(m1, s1, n1, m2, s2, n2, m0 = 0, equal.variance = FALSE) {
        # special case
        if (n1 <= 10 | n2 <= 10) {
            return(list(
                pValue = NA_real_,
                effectSize = NA_real_,
                standarizeMeanDifference = NA_real_,
                testName = "Welch Two Sample t-test (Error: Less than 10 samples)"
            ))
        }

        # special case
        if (m1 == 0 & m2 == 0 & s1 == 0 & s2 == 0) {
            return(list(
                pValue = 1,
                effectSize = 0,
                standarizeMeanDifference = 0,
                testName = "Welch Two Sample t-test"
            ))
        }

        if (equal.variance == FALSE) {
            se <- sqrt((s1^2 / n1) + (s2^2 / n2))
            # welch-satterthwaite df
            df <- ((s1^2 / n1 + s2^2 / n2)^2) / ((s1^2 / n1)^2 / (n1 - 1) + (s2^2 / n2)^2 / (n2 - 1))
        } else {
            # pooled standard deviation, scaled by the sample sizes
            se <- sqrt((1 / n1 + 1 / n2) * ((n1 - 1) * s1^2 + (n2 - 1) * s2^2) / (n1 + n2 - 2))
            df <- n1 + n2 - 2
        }
        t <- (m1 - m2 - m0) / se
        p <- 2 * pt(-abs(t), df)
        # dat <- c(m1-m2, se, t, 2*pt(-abs(t),df))
        # names(dat) <- c("Difference of means", "Std Error", "t", "p-value")
        return(list(
            pValue = p,
            effectSize = m1 - m2,
            standarizeMeanDifference = (m1 - m2) / sqrt((s1^2 + s2^2) / 2),
            testName = "Welch Two Sample t-test"
        ))
    }

    continuousCovariatesTible <- continuousCovariatesTible |>
        dplyr::bind_cols(
            continuousCovariatesTible |>
                dplyr::select(averageValue.x, standardDeviation.x, countValue.x, averageValue.y, standardDeviation.y, countValue.y) |>
                purrr::pmap_df(~ .welchTTest(..1, ..2, ..3, ..4, ..5, ..6))
        )

    return(continuousCovariatesTible)
}
