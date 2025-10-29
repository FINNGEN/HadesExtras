test_that("CohortTableHandler creates object with correct params", {
  suppressWarnings({
  cohortTableHandler <- helper_createNewCohortTableHandler(loadConnectionChecksLevel = "allChecks")
  })
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortTableHandler |> checkmate::expect_class("CohortTableHandler")
  cohortTableHandler$connectionStatusLog |> checkmate::expect_tibble()
  cohortTableHandler$connectionStatusLog |>
    dplyr::filter(type != "SUCCESS") |>
    nrow() |>
    expect_equal(0)

  # check returns empty valid tables
  cohortTableHandler$getCohortIdAndNames() |> checkmate::expect_tibble(max.rows = 0)
  cohortTableHandler$getCohortCounts() |> checkmate::expect_tibble(max.rows = 0)
  cohortTableHandler$getCohortsSummary() |> assertCohortsSummary()
})


test_that("CohortTableHandler works with loadConnectionChecksLevel basicChecks", {
  cohortTableHandler <- helper_createNewCohortTableHandler(loadConnectionChecksLevel = "basicChecks")

  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortTableHandler |> checkmate::expect_class("CohortTableHandler")
  cohortTableHandler$connectionStatusLog |> checkmate::expect_tibble()
  cohortTableHandler$connectionStatusLog |>
    dplyr::filter(type == "SUCCESS") |>
    nrow() |>
    expect_equal(4)
  cohortTableHandler$connectionStatusLog |>
    dplyr::filter(type == "WARNING") |>
    nrow() |>
    expect_equal(1)

  # check returns empty valid tables
  cohortTableHandler$getCohortIdAndNames() |> checkmate::expect_tibble(max.rows = 0)
  cohortTableHandler$getCohortCounts() |> checkmate::expect_tibble(max.rows = 0)
  cohortTableHandler$getCohortsSummary() |> assertCohortsSummary()
})

#
# insertOrUpdateCohorts
#
test_that("CohortTableHandler$insertOrUpdateCohorts adds a cohort", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts |> checkmate::expect_tibble(nrows = 1)
  cohortCounts$cohortName |> expect_equal("cohort1")
  cohortCounts$cohortId |> expect_equal(10)
  cohortCounts$cohortEntries |> expect_equal(1)
  cohortCounts$cohortSubjects |> expect_equal(1)
})

test_that("CohortTableHandler$insertOrUpdateCohorts warns when cohortId exists and upadte it", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts |> checkmate::expect_tibble(nrows = 1)
  cohortCounts$cohortName |> expect_equal("cohort1")
  cohortCounts$cohortId |> expect_equal(10)
  cohortCounts$cohortEntries |> expect_equal(1)
  cohortCounts$cohortSubjects |> expect_equal(1)

  cohortDefinitionSet$cohortName <- "cohort1 Updated"
  expect_warning(cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet))
  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts |> checkmate::expect_tibble(nrows = 1)
  cohortCounts$cohortName |> expect_equal("cohort1 Updated")
  cohortCounts$cohortId |> expect_equal(10)
  cohortCounts$cohortEntries |> expect_equal(1)
  cohortCounts$cohortSubjects |> expect_equal(1)
})

test_that("CohortTableHandler$insertOrUpdateCohorts keeps data when update an unchanged cohort", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortGeneratorResults <- cohortTableHandler$cohortGeneratorResults

  cohortGeneratorResults |> checkmate::expect_tibble(nrows = 1)
  cohortGeneratorResults$cohortId |> expect_equal(10)
  cohortGeneratorResults$generationStatus |> expect_equal("COMPLETE")


  expect_warning(cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet))

  cohortGeneratorResults <- cohortTableHandler$cohortGeneratorResults

  cohortGeneratorResults |> checkmate::expect_tibble(nrows = 1)
  cohortGeneratorResults$cohortId |> expect_equal(10)
  cohortGeneratorResults$generationStatus |> expect_equal("COMPLETE")
})


test_that("CohortTableHandler$insertOrUpdateCohorts errors with wrong sql", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE WRONG @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );",
    json = ""
  )

  expect_error(cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet))
})

test_that("CohortTableHandler$insertOrUpdateCohorts can insert a cohort with no subjects, and summary returns a sanitised tibble", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  cohortDefinitionSet <- tibble::tibble(
    cohortId = 10,
    cohortName = "cohort1",
    sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;",
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortsSummary <- cohortTableHandler$getCohortsSummary()

  cohortsSummary |>
    checkCohortsSummary() |>
    expect_true()
  cohortsSummary$cohortEntries |> expect_equal(0)
  cohortsSummary$cohortSubjects |> expect_equal(0)
  cohortsSummary$histogramCohortStartYear[[1]] |>
    nrow() |>
    expect_equal(0)
  cohortsSummary$histogramCohortEndYear[[1]] |>
    nrow() |>
    expect_equal(0)
  cohortsSummary$sexCounts[[1]] |>
    nrow() |>
    expect_equal(0)
  cohortsSummary$buildInfo[[1]]$logTibble$message |> expect_equal("Cohort is empty")
})

#
# Delete cohorts
#
#
# Delete cohorts
#
test_that("CohortTableHandler$deleteCohorts deletes a cohort and cohortsSummary", {
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
  )
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortTableHandler$getCohortsSummary() |>
    checkCohortsSummary() |>
    expect_true()
  cohortTableHandler$getCohortsSummary()$cohortId |> expect_equal(c(10, 20))
  cohortTableHandler$cohortDefinitionSet$cohortId |> expect_equal(c(10, 20))
  cohortTableHandler$cohortGeneratorResults$cohortId |> expect_equal(c(10, 20))
  cohortTableHandler$cohortDemograpics$cohortId |> expect_equal(c(10, 20))
  cohortTableHandler$getCohortsOverlap()$cohortIdCombinations |> expect_equal(c("-10-20-"))

  cohortTableHandler$deleteCohorts(10L)

  cohortTableHandler$getCohortsSummary() |>
    checkCohortsSummary() |>
    expect_true()
  cohortTableHandler$getCohortsSummary()$cohortId |> expect_equal(c(20))
  cohortTableHandler$cohortDefinitionSet$cohortId |> expect_equal(c(20))
  cohortTableHandler$cohortGeneratorResults$cohortId |> expect_equal(c(20))
  cohortTableHandler$cohortDemograpics$cohortId |> expect_equal(c(20))
  cohortTableHandler$getCohortsOverlap()$cohortIdCombinations |> expect_equal(c("-20-"))
})


test_that("CohortTableHandler$deleteCohorts deletes more thatn one cohort and cohortsSummary", {
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
  )
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortTableHandler$getCohortsSummary() |>
    checkCohortsSummary() |>
    expect_true()
  cohortTableHandler$getCohortsSummary()$cohortId |> expect_equal(c(10, 20))
  cohortTableHandler$cohortDefinitionSet$cohortId |> expect_equal(c(10, 20))
  cohortTableHandler$cohortGeneratorResults$cohortId |> expect_equal(c(10, 20))
  cohortTableHandler$cohortDemograpics$cohortId |> expect_equal(c(10, 20))
  cohortTableHandler$getCohortsOverlap()$cohortIdCombinations |> expect_equal(c("-10-20-"))

  cohortTableHandler$deleteCohorts(c(10L, 20L))

  cohortTableHandler$getCohortsSummary() |>
    checkCohortsSummary() |>
    expect_true()
  cohortTableHandler$getCohortsSummary() |> nrow() |> expect_equal(0)
})



#
# cohort summary
#
test_that("CohortTableHandler$cohortsSummary return a tibbe with data", {
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
  )
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortsSummary <- cohortTableHandler$getCohortsSummary()

  cohortsSummary |>
    checkCohortsSummary() |>
    expect_true()
})

#
# updateCohortNames
#
test_that("CohortTableHandler$updateCohortNames updates cohort names", {
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
  )
  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortTableHandler$updateCohortNames(10, "New Name", "NEWNAME")

  cohortTableHandler$getCohortIdAndNames() |> checkmate::expect_tibble(nrows = 2)
  cohortTableHandler$getCohortIdAndNames() |>
    dplyr::filter(cohortId == 10) |>
    pull(cohortName) |>
    expect_equal("New Name")
  cohortTableHandler$getCohortIdAndNames() |>
    dplyr::filter(cohortId == 10) |>
    pull(shortName) |>
    expect_equal("NEWNAME")
  cohortTableHandler$getCohortIdAndNames() |>
    dplyr::filter(cohortId == 20) |>
    pull(cohortName) |>
    expect_equal("cohort2")
  cohortTableHandler$getCohortIdAndNames() |>
    dplyr::filter(cohortId == 20) |>
    pull(shortName) |>
    expect_equal("coho")
})


#
# updateCohortNames
#
test_that("CohortTableHandler$updateCohortNames creates shortName one is NA", {
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

  cohortTableHandler$getCohortIdAndNames() |>
    dplyr::filter(cohortId == 10) |>
    pull(shortName) |>
    expect_equal("aa")
  cohortTableHandler$getCohortIdAndNames() |>
    dplyr::filter(cohortId == 20) |>
    pull(shortName) |>
    expect_equal("coho")
})


#
# getNumberOfSubjects
#
test_that("getNumberOfSubjects returns correct subject counts for cohort IDs", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  # SQL for cohort 10: 1 subject
  sql10 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );"

  # SQL for cohort 20: 2 subjects, 3 entries
  sql20 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)),
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)),
      (@target_cohort_id, 3, CAST('20000101' AS DATE), CAST('20220101' AS DATE));"

  cohortDefinitionSet <- tibble::tibble(
    cohortId = c(10, 20),
    cohortName = c("cohort1", "cohort2"),
    sql = c(sql10, sql20),
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  # checking number subjects and number of entries
  expect_equal(cohortTableHandler$getNumberOfSubjects(10), 1)
  expect_equal(cohortTableHandler$getNumberOfSubjects(20), 2)
  expect_equal(cohortTableHandler$getNumberOfCohortEntries(20), 3)

})


#
# getNumberOfCohortEntries
#
test_that("getNumberOfCohortEntries returns correct entry counts for cohort IDs", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  # SQL for cohort 10: 1 subject
  sql10 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );"

  # SQL for cohort 20: 2 subjects, 3 entries
  sql20 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)),
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)),
      (@target_cohort_id, 3, CAST('20000101' AS DATE), CAST('20220101' AS DATE));"

  cohortDefinitionSet <- tibble::tibble(
    cohortId = c(10, 20),
    cohortName = c("cohort1", "cohort2"),
    sql = c(sql10, sql20),
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  expect_equal(cohortTableHandler$getNumberOfCohortEntries(10), 1)
  expect_equal(cohortTableHandler$getNumberOfCohortEntries(20), 3)
})

#
# getNumberOfOverlappingSubjects
#
test_that("getNumberOfOverlappingSubjects returns correct number of overlapping subjects", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  # SQL for cohort 10: 1 subject
  sql10 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );"

  # SQL for cohort 20: 2 subjects, 3 entries
  sql20 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)),
      (@target_cohort_id, 1, CAST('20000104' AS DATE), CAST('20220107' AS DATE)),
      (@target_cohort_id, 3, CAST('20000105' AS DATE), CAST('20220106' AS DATE));"

  cohortDefinitionSet <- tibble::tibble(
    cohortId = c(10, 20),
    cohortName = c("cohort1", "cohort2"),
    sql = c(sql10, sql20),
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  expect_equal(cohortTableHandler$getNumberOfOverlappingSubjects(10,20), 1)
})

#
# getSexFisherTest
#
test_that("getSexFisherTest returns valid fisher.test result", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  # SQL for cohort 10: 1 subject
  sql10 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)  );"

  # SQL for cohort 20: 2 subjects, 3 entries
  sql20 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)),
      (@target_cohort_id, 1, CAST('20000104' AS DATE), CAST('20220107' AS DATE)),
      (@target_cohort_id, 3, CAST('20000105' AS DATE), CAST('20220106' AS DATE));"

  cohortDefinitionSet <- tibble::tibble(
    cohortId = c(10, 20),
    cohortName = c("cohort1", "cohort2"),
    sql = c(sql10, sql20),
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  # ToDO: Prepare summary with sexCounts nested tibbles with realistic data for testing
  #sexCounts1 <- tibble::tibble(sex = c("FEMALE","MALE"), n = c(30, 5))
  #sexCounts2 <- tibble::tibble(sex = c("FEMALE","MALE"), n = c(100, 300))
  #sexCounts = list(sexCounts1, sexCounts2)
  #cohortSummary_forTesting = cohortTableHandler$getCohortsSummary()
  #cohortSummary_forTesting$sexCounts = sexCounts

  res <- cohortTableHandler$getSexFisherTest(10, 20)

  expect_s3_class(res, "htest")
  expect_true("p.value" %in% names(res))
  expect_true(res$p.value >= 0 && res$p.value <= 1)
  expect_equal(res$p.value, 1)

})

#
# getYearOfBirthTests
#
test_that("getYearOfBirthTests returns t-test, ks-test, and cohen's d results", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  # SQL for cohort 10: 1 subject
  sql10 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
       (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)),
       (@target_cohort_id, 2, CAST('20000104' AS DATE), CAST('20220107' AS DATE));"

  # SQL for cohort 20: 2 subjects, 3 entries
  sql20 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20220101' AS DATE)),
      (@target_cohort_id, 1, CAST('20000104' AS DATE), CAST('20220107' AS DATE)),
      (@target_cohort_id, 3, CAST('20000105' AS DATE), CAST('20220106' AS DATE));"

  cohortDefinitionSet <- tibble::tibble(
    cohortId = c(10, 20),
    cohortName = c("cohort1", "cohort2"),
    sql = c(sql10, sql20),
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  results <- cohortTableHandler$getYearOfBirthTests(10,20)

  # check the returned result is a list with appropriate data types, and evaluate result p-values for the injected mock data
  # Assumes subjects 1, 2, and 3 have similar year_of_birth values in test data

  expect_type(results, "list")
  expect_s3_class(results$ttestResult, "htest")
  expect_s3_class(results$ksResult, "htest")
  expect_named(results$cohendResult, c("meanInCases", "meanInControls", "pooledsd", "cohend"))
  expect_true(is.numeric(results$cohendResult["cohend"]))
  expect_gt(results[["ttestResult"]]$p.value, 0.05)
  expect_gt(results[["ksResult"]]$p.value, 0.05)

})

#
# getYearOfBirthTests
#
test_that("getYearOfBirthTests handles edge case with one subject", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  withr::defer({
    rm(cohortTableHandler)
    gc()
  })

  # Cohort 10: only one subject → will cause t.test error if unguarded
  sql10 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (@target_cohort_id, 1, CAST('20000101' AS DATE), CAST('20200101' AS DATE));
  "

  # Cohort 20: two subjects → valid
  sql20 <- "
    DELETE FROM @target_database_schema.@target_cohort_table
    WHERE cohort_definition_id = @target_cohort_id;
    INSERT INTO @target_database_schema.@target_cohort_table
    (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (@target_cohort_id, 2, CAST('20010101' AS DATE), CAST('20210101' AS DATE)),
      (@target_cohort_id, 3, CAST('20010104' AS DATE), CAST('20210104' AS DATE));
  "

  cohortDefinitionSet <- tibble::tibble(
    cohortId = c(10, 20),
    cohortName = c("cohort1", "cohort2"),
    sql = c(sql10, sql20),
    json = ""
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  # Should not error
  results <- cohortTableHandler$getYearOfBirthTests(10, 20)

  # Should return NAs for ttestResult and cohendResult
  expect_true(is.na(results$ttestResult))
  expect_true(is.na(results$ksResult))
  expect_true(all(is.na(results$cohendResult)))
})



