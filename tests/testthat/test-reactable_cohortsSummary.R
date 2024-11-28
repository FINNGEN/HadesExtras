test_that("rectable_cohortsSummary works", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortDefinitionSet <- tibble::tibble(
    cohortId = c(10, 20),
    cohortName = c("cohort1", "cohort2"),
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  ) |>
    dplyr::mutate(shortName = c("aa", NA_character_))

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  # make one cohort empty
  cohortsSummary <- cohortTableHandler$getCohortsSummary() |>
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
