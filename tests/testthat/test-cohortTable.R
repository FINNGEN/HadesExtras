test_that("getCohortNamesFromCohortDefinitionTable returns a cohort", {
  connection <- helper_createNewConnection()

  cohortDatabaseSchema <- test_cohortTableHandlerConfig$cohortTable$cohortDatabaseSchema
  cdmDatabaseSchema <- test_cohortTableHandlerConfig$cdm$cdmDatabaseSchema
  cohortTableName <- helper_tableNameWithTimestamp("test_cohort")

  withr::defer({
    helper_dropTable(connection, cohortDatabaseSchema, cohortTableName)
    DatabaseConnector::dropEmulatedTempTables(connection)
    DatabaseConnector::disconnect(connection)
  })

  testCohortDefinitionTable <- tibble::tribble(
    ~cohort_definition_id, ~cohort_definition_name, ~cohort_definition_description, ~definition_type_concept_id, ~cohort_definition_syntax, ~subject_concept_id, ~cohort_initiation_date,
    1, "Diabetes Cohort", "Cohort of patients diagnosed with diabetes", 1234, 'SELECT * FROM patients WHERE diagnosis = "Diabetes"', 5678, as.Date("2022-01-01"),
    2, "Hypertension Cohort", "Cohort of patients diagnosed with hypertension", 1234, 'SELECT * FROM patients WHERE diagnosis = "Hypertension"', 5678, as.Date("2022-01-01"),
    3, "Obesity Cohort", "Cohort of patients diagnosed with obesity", 1234, 'SELECT * FROM patients WHERE diagnosis = "Obesity"', 5678, as.Date("2022-01-01")
  )

  suppressWarnings({
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = cohortDatabaseSchema,
      tableName = cohortTableName,
      data = testCohortDefinitionTable
    )
  })

  cohortNames <- getCohortNamesFromCohortDefinitionTable(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortDefinitionTable = cohortTableName
  )

  cohortNames |>
    dplyr::pull(cohort_definition_name) |>
    expect_equal(c("Diabetes Cohort", "Hypertension Cohort", "Obesity Cohort"))
})



test_that("Copy from cohortTable using insertOrUpdateCohorts works", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  connection <- cohortTableHandler$connectionHandler$getConnection()

  cohortDatabaseSchema <- cohortTableHandler$cohortDatabaseSchema
  cohortTableName <- helper_tableNameWithTimestamp("test_cohort")

  withr::defer({
    helper_dropTable(connection, cohortDatabaseSchema, cohortTableName)
    DatabaseConnector::dropEmulatedTempTables(cohortTableHandler$connectionHandler$getConnection())
    rm(cohortTableHandler)
    gc()
  })

  # set data
  testCohortTable <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1, 1, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 2, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 3, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 4, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2004-01-01"), as.Date("2004-12-01"),
    2, 2, as.Date("2001-01-01"), as.Date("2002-12-01"), # non overplaping
    2, 3, as.Date("2000-06-01"), as.Date("2000-09-01"), # inside
    2, 4, as.Date("2000-06-01"), as.Date("2010-12-01"), # overlap
    2, 5, as.Date("2004-06-01"), as.Date("2010-12-01"), # overlap with second
    2, 6, as.Date("2000-01-01"), as.Date("2010-12-01")
  ) |>
    dplyr::mutate(
      cohort_definition_id = as.integer(cohort_definition_id),
      subject_id = as.integer(subject_id)
    )

  suppressWarnings({
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = cohortDatabaseSchema,
      tableName = cohortTableName,
      data = testCohortTable
    )
  })

  testCohortDefinitionTable <- tibble::tribble(
    ~cohort_definition_id, ~cohort_definition_name, ~cohort_definition_description, ~definition_type_concept_id, ~cohort_definition_syntax, ~subject_concept_id, ~cohort_initiation_date,
    1, "Diabetes Cohort", "Cohort of patients diagnosed with diabetes", 1234, 'SELECT * FROM patients WHERE diagnosis = "Diabetes"', 5678, as.Date("2022-01-01"),
    2, "Hypertension Cohort", "Cohort of patients diagnosed with hypertension", 1234, 'SELECT * FROM patients WHERE diagnosis = "Hypertension"', 5678, as.Date("2022-01-01"),
    3, "Obesity Cohort", "Cohort of patients diagnosed with obesity", 1234, 'SELECT * FROM patients WHERE diagnosis = "Obesity"', 5678, as.Date("2022-01-01")
  )

  cohortDefinitionSet <- cohortTableToCohortDefinitionSettings(
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName,
    cohortDefinitionTable = testCohortDefinitionTable,
    cohortDefinitionIds = c(1, 2)
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts$cohortId |> expect_equal(c(1, 2))
})


test_that(" change cohort ids", {
  cohortTableHandler <- helper_createNewCohortTableHandler()
  connection <- cohortTableHandler$connectionHandler$getConnection()
  cohortDatabaseSchema <- cohortTableHandler$cohortDatabaseSchema
  cohortTableName <- helper_tableNameWithTimestamp("test_cohort")

  on.exit({
    helper_dropTable(connection, cohortDatabaseSchema, cohortTableName)
    DatabaseConnector::dropEmulatedTempTables(connection)
    rm(cohortTableHandler)
    gc()
  })

  # set data
  testCohortTable <- tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1, 1, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 2, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 3, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 4, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2004-01-01"), as.Date("2004-12-01"),
    2, 2, as.Date("2001-01-01"), as.Date("2002-12-01"), # non overplaping
    2, 3, as.Date("2000-06-01"), as.Date("2000-09-01"), # inside
    2, 4, as.Date("2000-06-01"), as.Date("2010-12-01"), # overlap
    2, 5, as.Date("2004-06-01"), as.Date("2010-12-01"), # overlap with second
    2, 6, as.Date("2000-01-01"), as.Date("2010-12-01")
  )|>
    dplyr::mutate(
      cohort_definition_id = as.integer(cohort_definition_id),
      subject_id = as.integer(subject_id)
    )

  suppressWarnings({
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = cohortDatabaseSchema,
      tableName = cohortTableName,
      data = testCohortTable
    )
  })

  testCohortDefinitionTable <- tibble::tribble(
    ~cohort_definition_id, ~cohort_definition_name, ~cohort_definition_description, ~definition_type_concept_id, ~cohort_definition_syntax, ~subject_concept_id, ~cohort_initiation_date,
    1, "Diabetes Cohort", "Cohort of patients diagnosed with diabetes", 1234, 'SELECT * FROM patients WHERE diagnosis = "Diabetes"', 5678, as.Date("2022-01-01"),
    2, "Hypertension Cohort", "Cohort of patients diagnosed with hypertension", 1234, 'SELECT * FROM patients WHERE diagnosis = "Hypertension"', 5678, as.Date("2022-01-01"),
    3, "Obesity Cohort", "Cohort of patients diagnosed with obesity", 1234, 'SELECT * FROM patients WHERE diagnosis = "Obesity"', 5678, as.Date("2022-01-01")
  )


  cohortDefinitionSet <- cohortTableToCohortDefinitionSettings(
    cohortDatabaseSchema = cohortTableHandler$cohortDatabaseSchema,
    cohortTable = cohortTableName,
    cohortDefinitionTable = testCohortDefinitionTable,
    cohortDefinitionIds = c(1, 2),
    newCohortDefinitionIds = c(10, 20)
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts$cohortId |> expect_equal(c(10, 20))
})
