plotCovariatesTestsAndromeda <- function(covariatesTestsAndromeda, comparisonIds = NULL, conceptIds = NULL) {
    #
    # Validate input
    #
    covariatesTestsAndromeda |> checkmate::assertClass("Andromeda")
    covariatesTestsAndromeda |>
        names() |>
        checkmate::assertSubset(c("analysisRef", "comparisons", "cohortCounts", "conceptRef", "covariates", "covariatesContinuous", "statisticalTests"))

    if (!is.null(comparisonIds)) {
        comparisonIds |> checkmate::assertSubset(covariatesTestsAndromeda$comparisons |> dplyr::pull(comparisonId))
        comparisons <- covariatesTestsAndromeda$comparisons |>
            dplyr::filter(comparisonId %in% comparisonIds)
    } else {
        comparisons <- covariatesTestsAndromeda$comparisons
    }

    if (!is.null(conceptIds)) {
        conceptIds |> checkmate::assertSubset(covariatesTestsAndromeda$conceptRef |> dplyr::pull(conceptId))
        covariates <- covariatesTestsAndromeda$covariates |>
            dplyr::filter(conceptId %in% conceptIds)
        covariatesContinuous <- covariatesTestsAndromeda$covariatesContinuous |>
            dplyr::filter(conceptId %in% conceptIds)
    } else {
        covariates <- covariatesTestsAndromeda$covariates
        covariatesContinuous <- covariatesTestsAndromeda$covariatesContinuous
    }


    #
    # Function
    #

    # Extract binary covariates
    analysisCaseControl <- covariatesTestsAndromeda$analysisRef |>
        dplyr::filter(analysisType == "Binary") |>
        dplyr::select(analysisId) |>
        dplyr::cross_join(comparisons)

    binaryCovariatesTible <- dplyr::full_join(
        # Case
        analysisCaseControl |>
            dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
            dplyr::inner_join(covariates, by = c("analysisId" = "analysisId", "caseCohortId" = "cohortDefinitionId")) |>
            dplyr::left_join(covariatesTestsAndromeda$cohortCounts, by = c("caseCohortId" = "cohortId")),
        # Control
        analysisCaseControl |>
            dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
            dplyr::inner_join(covariates, by = c("analysisId" = "analysisId", "controlCohortId" = "cohortDefinitionId")) |>
            dplyr::left_join(covariatesTestsAndromeda$cohortCounts, by = c("controlCohortId" = "cohortId")),
        #
        by = c("comparisonId", "analysisId", "conceptId", "caseCohortId", "controlCohortId")
    ) |>
        dplyr::transmute(
            comparisonId = comparisonId,
            analysisId = analysisId,
            unit = "",
            caseCohortId = caseCohortId,
            controlCohortId = controlCohortId,
            conceptId = conceptId,
            sumValue.x = dplyr::if_else(is.na(sumValue.x), 0, sumValue.x),
            sumValue.y = dplyr::if_else(is.na(sumValue.y), 0, sumValue.y),
            cohortSubjects.x = cohortSubjects.x,
            cohortSubjects.y = cohortSubjects.y
        ) |>
        dplyr::collect() |>
        dplyr::group_by(comparisonId, analysisId, unit, caseCohortId, controlCohortId, conceptId) |>
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
        ) |>
        #
        dplyr::mutate(
            n = purrr::map_chr(.x = contingencyTable, .f = ~ paste0(as.integer(.x[1, 1]), "<br>", as.integer(.x[2, 1]))),
            s = "",
            p = purrr::map_chr(.x = contingencyTable, .f = .createProportionBars)
        ) |>
        dplyr::select(-contingencyTable)

    # Extract categorical covariates
    analysisCaseControl <- covariatesTestsAndromeda$analysisRef |>
        dplyr::filter(analysisType == "Categorical") |>
        dplyr::select(analysisId) |>
        dplyr::cross_join(comparisons)

    categoricalCovariatesTible <- dplyr::full_join(
        # Case
        analysisCaseControl |>
            dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
            dplyr::inner_join(covariates, by = c("analysisId" = "analysisId", "caseCohortId" = "cohortDefinitionId")),
        # Control
        analysisCaseControl |>
            dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
            dplyr::inner_join(covariates, by = c("analysisId" = "analysisId", "controlCohortId" = "cohortDefinitionId")),
        #
        by = c("comparisonId", "analysisId", "conceptId", "categoryId", "caseCohortId", "controlCohortId")
    ) |>
        dplyr::transmute(
            comparisonId = comparisonId,
            analysisId = analysisId,
            unit = "",
            caseCohortId = caseCohortId,
            controlCohortId = controlCohortId,
            conceptId = conceptId,
            categoryId = categoryId,
            sumValue.x = dplyr::if_else(is.na(sumValue.x), 0, sumValue.x),
            sumValue.y = dplyr::if_else(is.na(sumValue.y), 0, sumValue.y)
        ) |>
        dplyr::collect() |>
        dplyr::group_by(comparisonId, analysisId, unit, caseCohortId, controlCohortId, conceptId) |>
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
        dplyr::filter(!purrr::map_lgl(contingencyTable, ~ is.null(.))) |>
        dplyr::mutate(
            n = purrr::map_chr(.x = contingencyTable, .f = ~ {
                mat <- .x
                # Remove column named '0' if present
                if ("0" %in% colnames(mat)) {
                    mat <- mat[, colnames(mat) != "0", drop = FALSE]
                }
                paste0(as.integer(mat[1, 1]), "<br>", as.integer(mat[2, 1]))
            }),
            s = "",
            p = purrr::map_chr(.x = contingencyTable, .f = .createProportionBarsTableN)
        ) |>
        dplyr::select(-contingencyTable)

    # Extract counts covariates
    analysisCaseControl <- covariatesTestsAndromeda$analysisRef |>
        dplyr::filter(analysisType %in% c("Counts", "Continuous", "AgeFirstEvent", "DaysToFirstEvent")) |>
        dplyr::select(analysisId) |>
        dplyr::cross_join(comparisons)

    continuousCovariatesTible <- dplyr::full_join(
        # Case
        analysisCaseControl |>
            dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
            dplyr::inner_join(covariatesContinuous, by = c("analysisId" = "analysisId", "caseCohortId" = "cohortDefinitionId")),
        # Control
        analysisCaseControl |>
            dplyr::select(comparisonId, analysisId, caseCohortId, controlCohortId) |>
            dplyr::inner_join(covariatesContinuous, by = c("analysisId" = "analysisId", "controlCohortId" = "cohortDefinitionId")),
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
            #
            meanValueCases = dplyr::if_else(is.na(averageValue.x), 0, averageValue.x),
            sdValueCases = dplyr::if_else(is.na(standardDeviation.x), 0, standardDeviation.x),
            nCasesYesWithValue = dplyr::if_else(is.na(countValue.x), 0, countValue.x),
            meanValueControls = dplyr::if_else(is.na(averageValue.y), 0, averageValue.y),
            sdValueControls = dplyr::if_else(is.na(standardDeviation.y), 0, standardDeviation.y),
            nControlsYesWithValue = dplyr::if_else(is.na(countValue.y), 0, countValue.y),
            #
            minValueCases = dplyr::if_else(is.na(minValue.x), 0, minValue.x),
            maxValueCases = dplyr::if_else(is.na(maxValue.x), 0, maxValue.x),
            minValueControls = dplyr::if_else(is.na(minValue.y), 0, minValue.y),
            maxValueControls = dplyr::if_else(is.na(maxValue.y), 0, maxValue.y),
            medianValueCases = dplyr::if_else(is.na(medianValue.x), 0, medianValue.x),
            medianValueControls = dplyr::if_else(is.na(medianValue.y), 0, medianValue.y),
            p10ValueCases = dplyr::if_else(is.na(p10Value.x), 0, p10Value.x),
            p10ValueControls = dplyr::if_else(is.na(p10Value.y), 0, p10Value.y),
            p25ValueCases = dplyr::if_else(is.na(p25Value.x), 0, p25Value.x),
            p25ValueControls = dplyr::if_else(is.na(p25Value.y), 0, p25Value.y),
            p75ValueCases = dplyr::if_else(is.na(p75Value.x), 0, p75Value.x),
            p75ValueControls = dplyr::if_else(is.na(p75Value.y), 0, p75Value.y),
            p90ValueCases = dplyr::if_else(is.na(p90Value.x), 0, p90Value.x),
            p90ValueControls = dplyr::if_else(is.na(p90Value.y), 0, p90Value.y)
        ) |>
        dplyr::collect() |>
        dplyr::group_by(comparisonId, analysisId, unit, caseCohortId, controlCohortId, conceptId) |>
        dplyr::summarize(
            n = paste0(as.integer(nCasesYesWithValue), "<br>", as.integer(nControlsYesWithValue)),
            s = paste0(round(meanValueCases, 2), " (", round(sdValueCases, 2), ")", "<br>", round(meanValueControls, 2), " (", round(sdValueControls, 2), ")"),
            p = purrr::pmap_chr(
                    .l = list(
                        medianValueCases, medianValueControls, p10ValueCases, p10ValueControls,
                        p25ValueCases, p25ValueControls, p75ValueCases, p75ValueControls,
                        p90ValueCases, p90ValueControls
                    ),
                    .f = .createBoxplot
                ), 
            .groups = "drop"
        )

    # Merge the three tibbles
    tibble <- dplyr::bind_rows(binaryCovariatesTible, categoricalCovariatesTible, continuousCovariatesTible)
    
    toPrintTible <- tibble |>
        dplyr::left_join(
            covariatesTestsAndromeda$statisticalTests |> dplyr::collect(),
            by = c("comparisonId", "analysisId", "unit", "caseCohortId", "controlCohortId", "conceptId")
        ) |>
        dplyr::select(-comparisonId, -caseCohortId, -controlCohortId) |>
        dplyr::left_join(
            covariatesTestsAndromeda$analysisRef |> dplyr::select(analysisId, analysisType, domainId) |> dplyr::collect(),
            by = c("analysisId" = "analysisId")
        ) |>
        dplyr::select(-analysisId) |>
        dplyr::left_join(
            covariatesTestsAndromeda$conceptRef |> dplyr::select(conceptId, conceptName, conceptCode) |> dplyr::collect(),
            by = c("conceptId" = "conceptId")
        ) |>
        dplyr::transmute(
            info = paste0(conceptName, "<br>", conceptCode, "<br>", domainId, "<br>", conceptId),
            analysisType = analysisType,
            n = n,
            s = s,
            p = p,
            mpLog10 = -log10(pValue) |> round(2),
            effectSize = effectSize |> round(2),
            standarizeMeanDifference = standarizeMeanDifference |> round(2),
            testName = testName
        )

    table <- reactable::reactable(
        toPrintTible,
        columns = list(
            info = reactable::colDef(
                html = TRUE, 
                minWidth = 300
            ),
            n = reactable::colDef(
                name = "n Cases <br> n Controls",
                html = TRUE
            ),
            s = reactable::colDef(
                name = "Mean (SD) Cases <br> Mean (SD) Controls",
                html = TRUE),
            p = reactable::colDef(
                name = "Distribution Cases<br>Distribution Controls",
                html = TRUE
            )
        ),
        filterable = TRUE,
        searchable = TRUE, 
        resizable = TRUE, 
        groupBy = c("info"),
        defaultExpanded = TRUE
    )

    # Save to temporary HTML file and open in browser
    temp_file <- tempfile(fileext = ".html")
    htmlwidgets::saveWidget(table, temp_file, selfcontained = TRUE)
    utils::browseURL(temp_file)

    return(table)
}


.createProportionBars <- function(contingencyTable, color_blue = "#00BFFF", color_grey = "#D3D3D3") {
    mat <- contingencyTable
    p_case <- mat[1, 1] / sum(mat[1, ])
    p_control <- mat[2, 1] / sum(mat[2, ])

    # Convert to percentages
    p_case_pct <- p_case * 100
    p_control_pct <- p_control * 100

    # Create HTML for two bars
    htmltools::HTML(paste0(
        '<div style="display: flex; flex-direction: column; gap: 4px; width: 100%;">',
        '<div style="height: 12px; width: 100%; background: linear-gradient(to right, ', color_blue, " ", p_case_pct, "%, ", color_grey, " ", p_case_pct, '%); border-radius: 2px;"></div>',
        '<div style="height: 12px; width: 100%; background: linear-gradient(to right, ', color_blue, " ", p_control_pct, "%, ", color_grey, " ", p_control_pct, '%); border-radius: 2px;"></div>',
        "</div>"
    ))
}


.createProportionBarsTableN <- function(contingencyTable, colors = NULL) {
    mat <- contingencyTable

    # If no columns left or only one column, return empty
    if (ncol(mat) == 0 || ncol(mat) == 1) {
        return(htmltools::HTML('<div style="height: 28px;"></div>'))
    }

    # Calculate proportions for each row
    row_sums_case <- sum(mat[1, ])
    row_sums_control <- sum(mat[2, ])

    # Handle zero sums
    if (row_sums_case == 0) row_sums_case <- 1
    if (row_sums_control == 0) row_sums_control <- 1

    props_case <- mat[1, ] / row_sums_case
    props_control <- mat[2, ] / row_sums_control

    # Generate color palette if not provided
    n_categories <- ncol(mat)
    if (is.null(colors)) {
        # Use a color palette - start with blue and vary hues
        colors <- grDevices::colorRampPalette(c("#00BFFF", "#4169E1", "#9370DB", "#FF6347", "#FFD700", "#32CD32", "#20B2AA"))(n_categories)
    } else if (length(colors) < n_categories) {
        # Recycle colors if not enough provided
        colors <- rep(colors, length.out = n_categories)
    }

    # Build gradient stops for case row
    case_stops <- character(n_categories)
    cumulative_pct_case <- 0
    for (i in seq_len(n_categories)) {
        pct <- props_case[i] * 100
        case_stops[i] <- paste0(colors[i], " ", cumulative_pct_case, "%, ", colors[i], " ", cumulative_pct_case + pct, "%")
        cumulative_pct_case <- cumulative_pct_case + pct
    }
    case_gradient <- paste(case_stops, collapse = ", ")

    # Build gradient stops for control row
    control_stops <- character(n_categories)
    cumulative_pct_control <- 0
    for (i in seq_len(n_categories)) {
        pct <- props_control[i] * 100
        control_stops[i] <- paste0(colors[i], " ", cumulative_pct_control, "%, ", colors[i], " ", cumulative_pct_control + pct, "%")
        cumulative_pct_control <- cumulative_pct_control + pct
    }
    control_gradient <- paste(control_stops, collapse = ", ")

    # Create HTML for two bars
    htmltools::HTML(paste0(
        '<div style="display: flex; flex-direction: column; gap: 4px; width: 100%;">',
        '<div style="height: 12px; width: 100%; background: linear-gradient(to right, ', case_gradient, '); border-radius: 2px;"></div>',
        '<div style="height: 12px; width: 100%; background: linear-gradient(to right, ', control_gradient, '); border-radius: 2px;"></div>',
        "</div>"
    ))
}

.createBoxplot <- function(
  medianValueCases,
  medianValueControls,
  p10ValueCases,
  p10ValueControls,
  p25ValueCases,
  p25ValueControls,
  p75ValueCases,
  p75ValueControls,
  p90ValueCases,
  p90ValueControls,
  width = 100,
  height = 40,
  color_cases = "#00BFFF",
  color_controls = "#4169E1"
) {
    # Calculate the overall range for scaling
    all_values <- c(
        p10ValueCases, p25ValueCases, medianValueCases, p75ValueCases, p90ValueCases,
        p10ValueControls, p25ValueControls, medianValueControls, p75ValueControls, p90ValueControls
    )

    # Handle edge case where all values are the same or zero
    if (all(all_values == 0) || max(all_values) == min(all_values)) {
        range_min <- 0
        range_max <- max(1, max(all_values))
    } else {
        range_min <- min(all_values)
        range_max <- max(all_values)
    }

    # Add padding
    range_span <- range_max - range_min
    if (range_span == 0) range_span <- 1
    padding <- range_span * 0.1
    range_min <- range_min - padding
    range_max <- range_max + padding

    # Helper function to scale a value to SVG coordinates
    scale_value <- function(value) {
        if (range_max == range_min) {
            return(width / 2)
        }
        ((value - range_min) / (range_max - range_min)) * width
    }

    # Scale all values
    p10_cases_x <- scale_value(p10ValueCases)
    p25_cases_x <- scale_value(p25ValueCases)
    median_cases_x <- scale_value(medianValueCases)
    p75_cases_x <- scale_value(p75ValueCases)
    p90_cases_x <- scale_value(p90ValueCases)

    p10_controls_x <- scale_value(p10ValueControls)
    p25_controls_x <- scale_value(p25ValueControls)
    median_controls_x <- scale_value(medianValueControls)
    p75_controls_x <- scale_value(p75ValueControls)
    p90_controls_x <- scale_value(p90ValueControls)

    # Box plot dimensions
    box_height <- 8
    case_y <- height / 4
    control_y <- 3 * height / 4

    # Create SVG for cases (top)
    # Whisker from p10 to p90
    case_whisker <- paste0(
        '<line x1="', p10_cases_x, '" y1="', case_y, '" x2="', p90_cases_x, '" y2="', case_y,
        '" stroke="', color_cases, '" stroke-width="1"/>'
    )

    # Left whisker cap (p10)
    case_left_cap <- paste0(
        '<line x1="', p10_cases_x, '" y1="', case_y - 2, '" x2="', p10_cases_x, '" y2="', case_y + 2,
        '" stroke="', color_cases, '" stroke-width="1"/>'
    )

    # Right whisker cap (p90)
    case_right_cap <- paste0(
        '<line x1="', p90_cases_x, '" y1="', case_y - 2, '" x2="', p90_cases_x, '" y2="', case_y + 2,
        '" stroke="', color_cases, '" stroke-width="1"/>'
    )

    # Box from p25 to p75
    case_box <- paste0(
        '<rect x="', p25_cases_x, '" y="', case_y - box_height / 2, '" width="', p75_cases_x - p25_cases_x,
        '" height="', box_height, '" fill="', color_cases, '" stroke="', color_cases, '" stroke-width="1" opacity="0.3"/>'
    )

    # Median line
    case_median <- paste0(
        '<line x1="', median_cases_x, '" y1="', case_y - box_height / 2, '" x2="', median_cases_x, '" y2="', case_y + box_height / 2,
        '" stroke="', color_cases, '" stroke-width="2"/>'
    )

    # Create SVG for controls (bottom)
    # Whisker from p10 to p90
    control_whisker <- paste0(
        '<line x1="', p10_controls_x, '" y1="', control_y, '" x2="', p90_controls_x, '" y2="', control_y,
        '" stroke="', color_controls, '" stroke-width="1"/>'
    )

    # Left whisker cap (p10)
    control_left_cap <- paste0(
        '<line x1="', p10_controls_x, '" y1="', control_y - 2, '" x2="', p10_controls_x, '" y2="', control_y + 2,
        '" stroke="', color_controls, '" stroke-width="1"/>'
    )

    # Right whisker cap (p90)
    control_right_cap <- paste0(
        '<line x1="', p90_controls_x, '" y1="', control_y - 2, '" x2="', p90_controls_x, '" y2="', control_y + 2,
        '" stroke="', color_controls, '" stroke-width="1"/>'
    )

    # Box from p25 to p75
    control_box <- paste0(
        '<rect x="', p25_controls_x, '" y="', control_y - box_height / 2, '" width="', p75_controls_x - p25_controls_x,
        '" height="', box_height, '" fill="', color_controls, '" stroke="', color_controls, '" stroke-width="1" opacity="0.3"/>'
    )

    # Median line
    control_median <- paste0(
        '<line x1="', median_controls_x, '" y1="', control_y - box_height / 2, '" x2="', median_controls_x, '" y2="', control_y + box_height / 2,
        '" stroke="', color_controls, '" stroke-width="2"/>'
    )

    # Combine all SVG elements
    svg_content <- paste0(
        '<svg width="', width, '" height="', height, '" xmlns="http://www.w3.org/2000/svg">',
        case_whisker, case_left_cap, case_right_cap, case_box, case_median,
        control_whisker, control_left_cap, control_right_cap, control_box, control_median,
        "</svg>"
    )

    htmltools::HTML(paste0(
        '<div style="display: flex; flex-direction: column; gap: 4px; width: 100%; align-items: center;">',
        svg_content,
        "</div>"
    ))
}
