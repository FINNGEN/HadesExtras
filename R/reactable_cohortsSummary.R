#' Cohorts Summary Table
#'
#' Generates a summary table of cohorts, including various cohort statistics.
#'
#' @param cohortsSummary A tibble in  cohortsSummary format.
#' @param deleteButtonsShinyId An optional Shiny input ID for handling the click of the delete buttons.
#' @param editButtonsShinyId An optional Shiny input ID for handling the click of the edit buttons.
#' 
#' @importFrom checkmate assertString
#' @importFrom dplyr mutate if_else select
#' @importFrom purrr map_chr
#' @importFrom reactable colDef
#' @importFrom htmltools tags
#'
#' @return A reactable table displaying cohort summary information.
#'
#' @export
rectable_cohortsSummary <- function(
    cohortsSummary,
    deleteButtonsShinyId = NULL,
    editButtonsShinyId = NULL) {
  cohortNameNcharLimit <- 15L

  cohortsSummary |> HadesExtras::assertCohortsSummary()
  deleteButtonsShinyId |> checkmate::assertString(null.ok = TRUE)
  editButtonsShinyId |> checkmate::assertString(null.ok = TRUE)

  cohortsSummaryToPlot <- cohortsSummary |>
    dplyr::mutate(
      databaseId = databaseId,
      databaseName = databaseName,
      shortName = shortName,
      fullName = cohortName,
      database = paste0(
        databaseId,
        "<br>",
        dplyr::if_else(nchar(databaseName) > cohortNameNcharLimit, paste0(substr(databaseName, 1, 15), "..."), databaseName)
      ),
      cohort = paste0(
        shortName,
        "<br>",
        dplyr::if_else(nchar(fullName) > cohortNameNcharLimit, paste0(substr(fullName, 1, 35), "..."), fullName)
      ),
      databaseTooltip = paste0(
        "Database Id: ", databaseId, "<br>",
        "Database Name: ", databaseName, "<br>"
      ),
      cohortTooltip = paste0(
        "Short name: ", shortName, "<br>",
        "Full name: ", fullName
      ),
      cohortCountsStr = paste0(
        cohortSubjects,
        "<br>",
        "(", cohortEntries, ")"
      ),
      cohortCountsStrTooltip = paste0(
        "Cohort has ", cohortEntries, " entries <br>",
        "from ", cohortSubjects, " unique subjects."
      ),
      histogramCohortStartYear,
      histogramCohortEndYear,
      countSexStr = purrr::map_chr(sexCounts, .sexTibbleToStr),
      countSexStrTooltip = purrr::map_chr(sexCounts, .sexTibbleToTooltipStr),
      buildInfoStr = purrr::map_chr(buildInfo, .buildInfoStr),
      buildInfoTooltip = purrr::map_chr(buildInfo, .buildInfoToTooltipStr),
      deleteButton = NA,
      editButton = NA
    )

  # add columns and onClick
  columns <- list(
    database = reactable::colDef(
      name = "Database",
      cell = function(value, index) {
        .tippyText(value, cohortsSummaryToPlot$databaseTooltip[[index]])
      },
      html = TRUE,
      maxWidth = 200
    ),
    cohort = reactable::colDef(
      name = "Cohort",
      cell = function(value, index) {
        .tippyText(value, cohortsSummaryToPlot$cohortTooltip[[index]])
      },
      html = TRUE
    ),
    cohortCountsStr = reactable::colDef(
      name = "N Subjects <br> (N Entries)",
      cell = function(value, index) {
        .tippyText(value, cohortsSummaryToPlot$cohortCountsStrTooltip[[index]])
      },
      html = TRUE,
      maxWidth = 100
    ),
    histogramCohortStartYear = reactable::colDef(
      name = "Cohort Start Date",
      cell = .render_apex_plot
    ),
    histogramCohortEndYear = reactable::colDef(
      name = "Cohort End Date",
      cell = .render_apex_plot
    ),
    countSexStr = reactable::colDef(
      name = "Sex",
      style = function(value) {
        .barStyle(perSexStr = value)
      },
      cell = function(value, index) {
        .tippyText(value, cohortsSummaryToPlot$countSexStrTooltip[[index]])
      },
      align = "left",
      maxWidth = 160
    ),
    buildInfoStr = reactable::colDef(
      name = "Build Info",
      cell = function(value, index) {
        .tippyText(value, cohortsSummaryToPlot$buildInfoTooltip[[index]])
      },
      html = TRUE,
      align = "center",
      maxWidth = 80
    )
  )

  if (!is.null(deleteButtonsShinyId)) {
    columns[["deleteButton"]] <- reactable::colDef(
      name = "",
      sortable = FALSE,
      cell = function() htmltools::tags$button(shiny::icon("trash")),
      maxWidth = 40
    )
  }

  if (!is.null(editButtonsShinyId)) {
    columns[["editButton"]] <- reactable::colDef(
      name = "",
      sortable = FALSE,
      cell = function() htmltools::tags$button(shiny::icon("edit")),
      maxWidth = 40
    )
  }

  onClick <- ""
  if (!is.null(deleteButtonsShinyId) | !is.null(editButtonsShinyId)) {
    onClick <- paste0(
      onClick,
      "function(rowInfo, column) {
          // Only handle click events on the 'details' column
          if (column.id == 'editButton') {
            if (window.Shiny) {
            Shiny.setInputValue('", editButtonsShinyId, "', { index: rowInfo.index + 1 }, { priority: 'event' })
            }
          }
          if (column.id == 'deleteButton') {
            if (window.Shiny) {
            Shiny.setInputValue('", deleteButtonsShinyId, "', { index: rowInfo.index + 1 }, { priority: 'event' })
            }
          }
          return false
        }
      "
    )
  }

  table <- cohortsSummaryToPlot |>
    dplyr::select(names(columns)) |>
    reactable::reactable(
      columns = columns,
      onClick = reactable::JS(onClick)
    )

  return(table)
}


#' Renders an Apex chart for the given data.
#'
#' @param data A data frame containing the data to be plotted.
#'
#' @importFrom apexcharter apex aes ax_chart ax_colors ax_yaxis
#'
#' @return An Apex chart object.
.render_apex_plot <- function(
    data) {
  colorTimeHist <- "#00BFFF"

  data |>
    apexcharter::apex(apexcharter::aes(year, n), type = "column", height = 50) |>
    apexcharter::ax_chart(sparkline = list(enabled = TRUE)) |>
    apexcharter::ax_colors(colorTimeHist) |>
    apexcharter::ax_yaxis(min = 0, max = ifelse(length(data$n) == 0, -Inf, max(data$n)))
}

#' Generate Style for Bar
#'
#' Generates a CSS style for a bar based on the percentage values of male, female, and NA.
#'
#' @param perSexStr A string representing the percentages of male, female, and NA (e.g., "40% 30% 30%").
#' @param colorSexMale The color for the male section (default is "#2c5e77").
#' @param colorSexFemale The color for the female section (default is "#BF616A").
#' @param colorSexNa The color for the NA section (default is "#8C8C8C").
#'
#' @return A list containing CSS style properties for the bar.
.barStyle <- function(
    perSexStr,
    colorSexMale = "#2c5e77",
    colorSexFemale = "#BF616A",
    colorSexNa = "#8C8C8C") {
  height <- "75%"
  fill_color <- colorSexMale
  background_color <- colorSexFemale
  na_color <- colorSexNa
  text_color <- "#FFFFFF"

  ss <- stringr::str_split(perSexStr, "[:blank:]")
  p_male <- ss[[1]][1] |>
    stringr::str_remove("%") |>
    as.double()
  p_na <- ss[[1]][2] |>
    stringr::str_remove("%") |>
    as.double()
  p_female <- ss[[1]][3] |>
    stringr::str_remove("%") |>
    as.double()

  list(
    backgroundImage = paste0(
      "linear-gradient(to right, ",
      fill_color, " ", p_male, "%, ",
      na_color, " ", p_male, "%, ", na_color, " ", p_male + p_na, "%, ",
      background_color, " ", p_male + p_na, "%, ",
      background_color,
      ")"
    ),
    backgroundSize = paste("100%", height),
    backgroundRepeat = "no-repeat",
    backgroundPosition = "center",
    color = text_color
  )
}

#' Convert Sex Data to String
#'
#' Converts sex data in a tibble to a string representation.
#'
#' @param data A tibble containing sex data.
#'
#' @return A string representation of the sex data (e.g., "40% 30% 30%").
#'
#' @importFrom dplyr filter
#' @importFrom purrr pluck
#'
.sexTibbleToStr <- function(data) {
  n_male <- data |>
    dplyr::filter(sex == "MALE") |>
    purrr::pluck("n", 1, .default = 0)
  n_female <- data |>
    dplyr::filter(sex == "FEMALE") |>
    purrr::pluck("n", 1, .default = 0)
  n_na <- data |>
    dplyr::filter(sex != "MALE" & sex != "FEMALE") |>
    purrr::pluck("n", 1, .default = 0)
  n_total <- n_male + n_female + n_na

  p_male <- round(n_male / n_total * 10000) / 100
  p_female <- round(n_female / n_total * 10000) / 100
  p_na <- round(n_na / n_total * 10000) / 100

  return(paste0(p_male, "% ", p_na, "% ", p_female, "%"))
}

#' Convert Sex Data to Tooltip String
#'
#' Converts sex data in a tibble to a tooltip string representation.
#'
#' @param data A tibble containing sex data.
#'
#' @return A tooltip string representation of the sex data.
#'
#' @importFrom dplyr filter
#' @importFrom purrr pluck
#'
.sexTibbleToTooltipStr <- function(data) {
  n_male <- data |>
    dplyr::filter(sex == "MALE") |>
    purrr::pluck("n", 1, .default = 0)
  n_female <- data |>
    dplyr::filter(sex == "FEMALE") |>
    purrr::pluck("n", 1, .default = 0)
  n_na <- data |>
    dplyr::filter(sex != "MALE" & sex != "FEMALE") |>
    purrr::pluck("n", 1, .default = 0)
  n_total <- n_male + n_female + n_na

  p_male <- round(n_male / n_total * 10000) / 100
  p_female <- round(n_female / n_total * 10000) / 100
  p_na <- round(n_na / n_total * 10000) / 100

  return(paste0(
    "Males: ", n_male, " (", p_male, "%)<br>",
    "Unknow: ", n_na, " (", p_na, "%)<br>",
    "Female: ", n_female, " (", p_female, "%)<br>"
  ))
}


#' Generate Tippy Text
#'
#' Generates tippy text with a tooltip for a given text.
#'
#' @param text The text to display.
#' @param tooltip The tooltip text.
#'
#' @return A tippy object.
#'
#' @importFrom tippy tippy
#'
.tippyText <- function(text, tooltip) {
  tippy::tippy(
    text = text,
    tooltip = paste0("<div style='text-align: left; font-size:16px;'>", tooltip, "<div>"),
    allowHTML = TRUE,
    theme = "light",
    arrow = TRUE
  )
}


#' Converts build information to a string representation.
#'
#' @param buildInfo Build information containing logs.
#'
#' @return A string representation of build information.
#'
#' @importFrom dplyr count mutate
#'
.buildInfoStr <- function(buildInfo) {
  # 
  emojis <- list(
    error = "\u274c",
    warning = "\u26A0\uFE0F",
    success = "\u2705",
    info = "\u2139\uFE0F"
  )

  buildInfo$logTibble |>
    dplyr::count(type) |>
    dplyr::mutate(
      type = dplyr::case_when(
        type == "INFO" ~ emojis$info,
        type == "SUCCESS" ~ emojis$success,
        type == "WARNING" ~ emojis$warning,
        type == "ERROR" ~ emojis$error
      )
    ) |>
    dplyr::transmute(
      str = paste0(type, "(", n, ")")
    ) |>
    dplyr::pull(str) |>
    paste(collapse = ", ")
}

#' Converts build information to a tooltip string representation.
#'
#' @param buildInfo Build information containing logs.
#'
#' @return A tooltip string representation of build information.
#'
#' @importFrom dplyr mutate
#'
.buildInfoToTooltipStr <- function(buildInfo) {
  emojis <- list(
    error = "\u274c",
    warning = "\u26A0\uFE0F",
    success = "\u2705",
    info = "\u2139\uFE0F"
  )

  buildInfo$logTibble |>
    dplyr::mutate(
      type = dplyr::case_when(
        type == "INFO" ~ emojis$info,
        type == "SUCCESS" ~ emojis$success,
        type == "WARNING" ~ emojis$warning,
        type == "ERROR" ~ emojis$error
      )
    ) |>
    dplyr::transmute(
      str = paste0(type, " ", message)
    ) |>
    dplyr::pull(str) |>
    paste(collapse = "<br>")
}
