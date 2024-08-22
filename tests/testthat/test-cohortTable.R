
test_that("getCohortNamesFromCohortDefinitionTable returns a cohort", {

  connection <- helper_createNewConnection()
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  # set data
  testCohortTable <-  tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1, 1, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 2, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 3, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 4, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2004-01-01"), as.Date("2004-12-01"),

    2, 2, as.Date("2001-01-01"), as.Date("2002-12-01"),# non overplaping
    2, 3, as.Date("2000-06-01"), as.Date("2000-09-01"),# inside
    2, 4, as.Date("2000-06-01"), as.Date("2010-12-01"),# overlap
    2, 5, as.Date("2004-06-01"), as.Date("2010-12-01"),# overlap with second
    2, 6, as.Date("2000-01-01"), as.Date("2010-12-01")
  )

  testCohortDefinitionTable <- tibble::tribble(
    ~cohort_definition_id, ~cohort_definition_name, ~cohort_definition_description, ~definition_type_concept_id, ~cohort_definition_syntax, ~subject_concept_id, ~cohort_initiation_date,
    1, 'Diabetes Cohort', 'Cohort of patients diagnosed with diabetes', 1234, 'SELECT * FROM patients WHERE diagnosis = "Diabetes"', 5678, as.Date('2022-01-01'),
    2, 'Hypertension Cohort', 'Cohort of patients diagnosed with hypertension', 1234, 'SELECT * FROM patients WHERE diagnosis = "Hypertension"', 5678, as.Date('2022-01-01'),
    3, 'Obesity Cohort', 'Cohort of patients diagnosed with obesity', 1234, 'SELECT * FROM patients WHERE diagnosis = "Obesity"', 5678, as.Date('2022-01-01')
  )


  DatabaseConnector::insertTable(
    connection = connection,
    table = "cohort_definition",
    data = testCohortDefinitionTable
  )

  cohortNames <- getCohortNamesFromCohortDefinitionTable(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortDefinitionTable = "cohort_definition"
  )

})



test_that("getCohortNamesFromCohortDefinitionTable returns a cohort", {

  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  # set data
  testCohortTable <-  tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1, 1, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 2, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 3, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 4, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2004-01-01"), as.Date("2004-12-01"),

    2, 2, as.Date("2001-01-01"), as.Date("2002-12-01"),# non overplaping
    2, 3, as.Date("2000-06-01"), as.Date("2000-09-01"),# inside
    2, 4, as.Date("2000-06-01"), as.Date("2010-12-01"),# overlap
    2, 5, as.Date("2004-06-01"), as.Date("2010-12-01"),# overlap with second
    2, 6, as.Date("2000-01-01"), as.Date("2010-12-01")
  )

  DatabaseConnector::insertTable(
    connection = cohortTableHandler$connectionHandler$getConnection(),
    table = "cohort",
    data = testCohortTable
  )

  testCohortDefinitionTable <- tibble::tribble(
    ~cohort_definition_id, ~cohort_definition_name, ~cohort_definition_description, ~definition_type_concept_id, ~cohort_definition_syntax, ~subject_concept_id, ~cohort_initiation_date,
    1, 'Diabetes Cohort', 'Cohort of patients diagnosed with diabetes', 1234, 'SELECT * FROM patients WHERE diagnosis = "Diabetes"', 5678, as.Date('2022-01-01'),
    2, 'Hypertension Cohort', 'Cohort of patients diagnosed with hypertension', 1234, 'SELECT * FROM patients WHERE diagnosis = "Hypertension"', 5678, as.Date('2022-01-01'),
    3, 'Obesity Cohort', 'Cohort of patients diagnosed with obesity', 1234, 'SELECT * FROM patients WHERE diagnosis = "Obesity"', 5678, as.Date('2022-01-01')
  )


  cohortDefinitionSet  <- cohortTableToCohortDefinitionSettings(
    cohortDatabaseSchema = cohortTableHandler$cohortDatabaseSchema,
    cohortTable = "cohort",
    cohortDefinitionTable = testCohortDefinitionTable,
    cohortDefinitionIds = c(1,2)
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts$cohortId |> expect_equal(c(1,2))

})


test_that(" change cohort ids", {

  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  # set data
  testCohortTable <-  tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1, 1, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 2, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 3, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 4, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2004-01-01"), as.Date("2004-12-01"),

    2, 2, as.Date("2001-01-01"), as.Date("2002-12-01"),# non overplaping
    2, 3, as.Date("2000-06-01"), as.Date("2000-09-01"),# inside
    2, 4, as.Date("2000-06-01"), as.Date("2010-12-01"),# overlap
    2, 5, as.Date("2004-06-01"), as.Date("2010-12-01"),# overlap with second
    2, 6, as.Date("2000-01-01"), as.Date("2010-12-01")
  )

  DatabaseConnector::insertTable(
    connection = cohortTableHandler$connectionHandler$getConnection(),
    table = "cohort",
    data = testCohortTable
  )

  testCohortDefinitionTable <- tibble::tribble(
    ~cohort_definition_id, ~cohort_definition_name, ~cohort_definition_description, ~definition_type_concept_id, ~cohort_definition_syntax, ~subject_concept_id, ~cohort_initiation_date,
    1, 'Diabetes Cohort', 'Cohort of patients diagnosed with diabetes', 1234, 'SELECT * FROM patients WHERE diagnosis = "Diabetes"', 5678, as.Date('2022-01-01'),
    2, 'Hypertension Cohort', 'Cohort of patients diagnosed with hypertension', 1234, 'SELECT * FROM patients WHERE diagnosis = "Hypertension"', 5678, as.Date('2022-01-01'),
    3, 'Obesity Cohort', 'Cohort of patients diagnosed with obesity', 1234, 'SELECT * FROM patients WHERE diagnosis = "Obesity"', 5678, as.Date('2022-01-01')
  )


  cohortDefinitionSet  <- cohortTableToCohortDefinitionSettings(
    cohortDatabaseSchema = cohortTableHandler$cohortDatabaseSchema,
    cohortTable = "cohort",
    cohortDefinitionTable = testCohortDefinitionTable,
    cohortDefinitionIds = c(1,2),
    newCohortDefinitionIds = c(10,20)
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  cohortCounts <- cohortTableHandler$getCohortCounts()

  cohortCounts$cohortId |> expect_equal(c(10,20))
})



