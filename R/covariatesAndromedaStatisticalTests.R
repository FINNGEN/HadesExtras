addStatisticalTestsToCovariatesAndromeda <- function(
  covariatesAndromeda,
  caseControlTible,
  analysisTypes = c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous")
) {
    covariatesAndromeda |> checkmate::assertClass("Andromeda")
    caseControlTible |> names()  |> checkmate::assertSubset(c("caseCohortId", "controlCohortId"))

    covariatesAndromeda$caseControl <- caseControlTible

    covariatesAndromeda$statisticalTests <- tibble::tibble(
        analysisId = integer(),
        caseCohortId = integer(),
        controlCohortId = integer(),
        conceptId = integer(),
        pValue = numeric(),
        oddsRatio = numeric(),
        testName = character()
    )

    if ("Binary" %in% analysisTypes) {
        binaryCovariatesTible <- covariatesAndromeda$analysisRef |>
            dplyr::filter(analysisType == "Binary") |>
            dplyr::select(analysisId) |>
            dplyr::cross_join(covariatesAndromeda$caseControl) |>
            dplyr::left_join(covariatesAndromeda$covariates, by = c("analysisId" = "analysisId", "caseCohortId" = "cohortDefinitionId")) |>
            dplyr::left_join(covariatesAndromeda$cohortCounts, by = c("caseCohortId" = "cohortId")) |>
            dplyr::left_join(covariatesAndromeda$covariates, by = c("analysisId" = "analysisId", "controlCohortId" = "cohortDefinitionId", "conceptId" = "conceptId")) |>
            dplyr::left_join(covariatesAndromeda$cohortCounts, by = c("controlCohortId" = "cohortId")) |>
            dplyr::transmute(
                analysisId = analysisId,
                caseCohortId = caseCohortId,
                controlCohortId = controlCohortId,
                conceptId = conceptId,
                nCasesYes = sumValue.x,
                nCasesNo = cohortSubjects.x - sumValue.x,
                nControlsYes = sumValue.y,
                nControlsNo = cohortSubjects.y - sumValue.y
            ) |>
            dplyr::collect()

        binaryCovariatesTible <- .addStatisticalTestsParallel(binaryCovariatesTible, "Binary")
        binaryCovariatesTible <- binaryCovariatesTible |>
            dplyr::select(analysisId, caseCohortId, controlCohortId, conceptId, pValue, oddsRatio, testName)

        Andromeda::appendToTable(covariatesAndromeda$statisticalTests, binaryCovariatesTible)
        rm(binaryCovariatesTible)
    }


    # add statistical tests results to covariatesAndromeda
    
    


    return(covariatesAndromeda)
}


#' @title parallelAddTestToCodeCounts
#' @description Runs .addTestToCodeCounts in parallel on chunks of the input tibble.
#' @param tibbleWithCodeCounts The tibble obtained from getCodeCounts.
#' @param n_chunks Number of chunks to split the tibble into (default: number of available cores).
#' @return Tibble with added p-value, odds ratio, and test columns.
#' @export
#' @importFrom furrr future_map_dfr
#' @importFrom dplyr bind_rows
#' @importFrom future plan multisession
.addStatisticalTestsParallel <- function(covariatesTible, analysisType, n_chunks = future::availableCores() - 1, ...) {
    # Load required packages
    requireNamespace("furrr", quietly = TRUE)
    requireNamespace("future", quietly = TRUE)

    analysisType |> checkmate::assertChoice(c("Binary", "Categorical", "Counts", "AgeFirstEvent", "DaysToFirstEvent", "Continuous"))
    n_chunks |> checkmate::assertNumber(upper = future::availableCores() - 1)

    if (analysisType == "Binary") {
        covariatesTible |> checkmate::assertTibble()
        covariatesTible |>
            names() |>
            checkmate::testSubset(
                c("analysisId", "caseCohortId", "controlCohortId", "conceptId", "nCasesYes", "nCasesNo", "nControlsYes", "nControlsNo")
            )
        fun <- .addFisherOrChiSquareTest
    }

    # Set up parallel plan
    future::plan(future::multisession, workers = n_chunks)

    # Split tibble into chunks
    chunks <- split(
        covariatesTible,
        cut(seq_len(nrow(covariatesTible)), breaks = n_chunks, labels = FALSE)
    )

    # Process each chunk in parallel
    result <- furrr::future_map_dfr(chunks, fun, ..., .options = furrr::furrr_options(seed = TRUE))

    return(result)
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
.addFisherOrChiSquareTest <- function(binaryCovariatesTible) {
    missing_collumns <- dplyr::setdiff(c("nCasesYes", "nCasesNo", "nControlsYes", "nControlsNo"), names(binaryCovariatesTible))
    if (length(missing_collumns) != 0) {
        stop("case_controls_counts is missing the following columns: ", missing_collumns)
    }

    # Needs to be in the same function to be used in parallel
    .fisher <- function(a, b, c, d) {
        data <- matrix(c(a, b, c, d), ncol = 2)
        if (a < 10 | b < 10 | c < 10 | d < 10) {
            results <- stats::fisher.test(data)
            p.value <- results$p.value
            odds.ratio <- results$estimate
            test <- "Fisher"
        } else {
            isWarn <- FALSE
            tryCatch(
                {
                    results <- chisq.test(data)
                    p.value <- results$p.value
                    odds.ratio <- (a * d) / (b * c)
                    test <- "Chi-square"
                },
                warning = function(w) {
                    isWarn <<- TRUE
                }
            )
            if (isWarn) {
                results <- stats::fisher.test(data)
                p.value <- results$p.value
                odds.ratio <- results$estimate
                test <- "Fisher"
            }
        }
        return(list(
            pValue = p.value,
            oddsRatio = odds.ratio,
            testName = test
        ))
    }

    binaryCovariatesTible <- binaryCovariatesTible |>
        dplyr::bind_cols(
            binaryCovariatesTible |>
                dplyr::select(nCasesYes, nCasesNo, nControlsYes, nControlsNo) |>
                purrr::pmap_df(~ .fisher(..1, ..2, ..3, ..4))
        )

    return(binaryCovariatesTible)
}



.addTTestToCodeCounts <- function(
  CodeCounts,
  columns = c(
      "mean_value_cases", "mean_value_controls",
      "sd_value_cases", "sd_value_controls",
      "n_cases_yes_with_value", "n_controls_yes_with_value"
  )
) {
    list(columns)

    checkmate::assertDataFrame(CodeCounts)
    CodeCounts |>
        names() |>
        checkmate::testSubset(columns)

    CodeCounts <- CodeCounts |>
        dplyr::bind_cols(
            CodeCounts |>
                dplyr::select(all_of(columns)) |>
                purrr::pmap_df(~ .t.test(..1, ..2, ..3, ..4, ..5, ..6))
        )

    return(CodeCounts)
}


# m1, m2: the sample means
# s1, s2: the sample standard deviations
# n1, n2: the same sizes
# m0: the null value for the difference in means to be tested for. Default is 0.
# equal.variance: whether or not to assume equal variance. Default is FALSE.
.t.test <- function(m1, m2, s1, s2, n1, n2, m0 = 0, equal.variance = FALSE) {
    # special case
    if (n1 <= 10 | n2 <= 10) {
        return(list(
            values_p_value = NA_real_,
            values_SE = NA_real_,
            values_test = "no test, less than 10 samples"
        ))
    }

    # special case
    if (m1 == 0 & m2 == 0 & s1 == 0 & s2 == 0) {
        return(list(
            values_p_value = 1,
            values_SE = 0,
            values_test = "Welch Two Sample t-test"
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
    # dat <- c(m1-m2, se, t, 2*pt(-abs(t),df))
    # names(dat) <- c("Difference of means", "Std Error", "t", "p-value")
    return(list(
        values_p_value = 2 * pt(-abs(t), df),
        values_SE = se,
        values_test = ifelse(is.na(se), "", "Welch Two Sample t-test")
    ))
}


.addPoissonTestToCodeCounts <- function(
  timeCodeWasCounts,
  columns = c("n_cases_yes", "total_n_events_cases", "n_controls_yes", "total_n_events_controls")
) {
    missing_collumns <- dplyr::setdiff(columns, names(timeCodeWasCounts))
    if (length(missing_collumns) != 0) {
        stop("case_controls_counts is missing the following columns: ", missing_collumns)
    }


    timeCodeWasCounts <- timeCodeWasCounts |>
        dplyr::bind_cols(
            timeCodeWasCounts |>
                dplyr::select(all_of(columns)) |>
                purrr::pmap_df(~ .poisson(..1, ..2, ..3, ..4))
        )

    return(timeCodeWasCounts)
}


.poisson <- function(a, b, c, d) {
    r <- poisson.test(c(b, d), c(a, c))

    return(list(
        n_events_p_value = r$p.value,
        n_events_rate_ratio = (b / a) / (c / d),
        n_events_test = "Poisson Test"
    ))
}
