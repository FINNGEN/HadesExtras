test_that("rectable_cohortsSummary works", {
  cohortTableHandler <- helper_createNewCohortTableHandler(addCohorts = "EunomiaDefaultCohorts")
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortsSummary <- cohortTableHandler$getCohortsSummary()

  # make one cohort empty
  cohortsSummary <- cohortsSummary |>
    dplyr::mutate(
      cohortEntries = dplyr::if_else(cohortId == 3, NA, cohortEntries),
      cohortSubjects = dplyr::if_else(cohortId == 3, NA, cohortSubjects)
    ) |>
    HadesExtras::correctEmptyCohortsInCohortsSummary()

  # table -------------------------------------------------------------------
  reactableResult <- rectable_cohortsSummary(cohortsSummary) # ,  deleteButtonsShinyId = "test")

  reactableResult |> checkmate::expect_class(classes = c("reactable", "htmlwidget"))
  reactableResult$x$tag$attribs$columns |>
    length() |>
    expect_equal(7)

  #
  reactableResult <- rectable_cohortsSummary(cohortsSummary, deleteButtonsShinyId = "test")

  reactableResult |> checkmate::expect_class(classes = c("reactable", "htmlwidget"))
  reactableResult$x$tag$attribs$columns |>
    length() |>
    expect_equal(8)

  #
  reactableResult <- rectable_cohortsSummary(cohortsSummary, deleteButtonsShinyId = "test", editButtonsShinyId = "test2")

  reactableResult |> checkmate::expect_class(classes = c("reactable", "htmlwidget"))
  reactableResult$x$tag$attribs$columns |>
    length() |>
    expect_equal(9)
})
