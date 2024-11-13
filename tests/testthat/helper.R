helper_createNewConnection <- function() {

  # by default use the one from setup.R
  connectionDetailsSettings <- test_cohortTableHandlerConfig$connection$connectionDetailsSettings

  if (!is.null(test_cohortTableHandlerConfig$connection$tempEmulationSchema)) {
    options(sqlRenderTempEmulationSchema = test_cohortTableHandlerConfig$connection$tempEmulationSchema)
  } else {
    options(sqlRenderTempEmulationSchema = NULL)
  }

  # create connection
  if (!is.null(connectionDetailsSettings$drv)) {
    # IBD connection details
    eval(parse(text = paste0("tmpDriverVar <- ", connectionDetailsSettings$drv)))
    connectionDetailsSettings$drv  <- tmpDriverVar
    connectionDetails <- rlang::exec(DatabaseConnector::createDbiConnectionDetails, !!!connectionDetailsSettings)
  } else {
    # JDBC connection details
    connectionDetails <- rlang::exec(DatabaseConnector::createConnectionDetails, !!!connectionDetailsSettings)
  }

  connection <- DatabaseConnector::connect(connectionDetails)

  return(connection)
}

helper_createNewCohortTableHandler <- function(addCohorts = NULL, loadConnectionChecksLevel = "basicChecks") {
  addCohorts |> checkmate::assertCharacter(len = 1, null.ok = TRUE)
  addCohorts |> checkmate::assertSubset(c(
    "EunomiaDefaultCohorts", "HadesExtrasFractureCohorts", "HadesExtrasAsthmaCohorts",
    "HadesExtrasFractureCohortsMatched", "HadesExtrasAsthmaCohortsMatched"
  ), empty.ok = TRUE)

  # by default use the one from setup.R
  cohortTableHandlerConfig <- test_cohortTableHandlerConfig # set by setup.R

  cohortTableHandler <- createCohortTableHandlerFromList(cohortTableHandlerConfig, loadConnectionChecksLevel)

  if (!is.null(addCohorts)) {
    if (addCohorts == "EunomiaDefaultCohorts") {
      cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
        settingsFileName = "testdata/name/Cohorts.csv",
        jsonFolder = "testdata/name/cohorts",
        sqlFolder = "testdata/name/sql/sql_server",
        cohortFileNameFormat = "%s",
        cohortFileNameValue = c("cohortName"),
        packageName = "CohortGenerator",
        verbose = FALSE
      )
    }
    if (addCohorts == "HadesExtrasFractureCohorts") {
      cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
        settingsFileName = "testdata/fracture/Cohorts.csv",
        jsonFolder = "testdata/fracture/cohorts",
        sqlFolder = "testdata/fracture/sql/sql_server",
        cohortFileNameFormat = "%s",
        cohortFileNameValue = c("cohortId"),
        subsetJsonFolder = "testdata/fracture/cohort_subset_definitions/",
        packageName = "HadesExtras",
        verbose = T
      )
    }
    if (addCohorts == "HadesExtrasAsthmaCohorts") {
      cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
        settingsFileName = "testdata/asthma/Cohorts.csv",
        jsonFolder = "testdata/asthma/cohorts",
        sqlFolder = "testdata/asthma/sql/sql_server",
        cohortFileNameFormat = "%s",
        cohortFileNameValue = c("cohortId"),
        subsetJsonFolder = "testdata/asthma/cohort_subset_definitions/",
        packageName = "HadesExtras",
        verbose = FALSE
      )
    }
    if (addCohorts == "HadesExtrasFractureCohortsMatched") {
      cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
        settingsFileName = "testdata/fracture/Cohorts.csv",
        jsonFolder = "testdata/fracture/cohorts",
        sqlFolder = "testdata/fracture/sql/sql_server",
        cohortFileNameFormat = "%s",
        cohortFileNameValue = c("cohortId"),
        subsetJsonFolder = "testdata/fracture/cohort_subset_definitions/",
        packageName = "HadesExtras",
        verbose = T
      )

      # Match
      subsetDef <- CohortGenerator::createCohortSubsetDefinition(
        name = "",
        definitionId = 1,
        subsetOperators = list(
          HadesExtras::createMatchingSubset(
            matchToCohortId = 1,
            matchRatio = 10,
            matchSex = TRUE,
            matchBirthYear = TRUE,
            matchCohortStartDateWithInDuration = FALSE,
            newCohortStartDate = "asMatch",
            newCohortEndDate = "keep"
          )
        )
      )

      cohortDefinitionSet <- cohortDefinitionSet |>
        CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 2)
    }
    if (addCohorts == "HadesExtrasAsthmaCohortsMatched") {
      # cohorts from eunomia
      cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
        settingsFileName = "testdata/asthma/Cohorts.csv",
        jsonFolder = "testdata/asthma/cohorts",
        sqlFolder = "testdata/asthma/sql/sql_server",
        cohortFileNameFormat = "%s",
        cohortFileNameValue = c("cohortId"),
        subsetJsonFolder = "testdata/asthma/cohort_subset_definitions/",
        packageName = "HadesExtras",
        verbose = FALSE
      )

      # Match to sex and bday, match ratio 10
      subsetDef <- CohortGenerator::createCohortSubsetDefinition(
        name = "",
        definitionId = 1,
        subsetOperators = list(
          HadesExtras::createMatchingSubset(
            matchToCohortId = 1,
            matchRatio = 10,
            matchSex = TRUE,
            matchBirthYear = TRUE,
            matchCohortStartDateWithInDuration = FALSE,
            newCohortStartDate = "asMatch",
            newCohortEndDate = "keep"
          )
        )
      )

      cohortDefinitionSet <- cohortDefinitionSet |>
        CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 2)
    }

    cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  }

  return(cohortTableHandler)
}


helper_getParedSourcePersonAndPersonIds <- function(
    connection,
    cdmDatabaseSchema,
    numberPersons) {
  # Connect, collect tables
  personTable <- dplyr::tbl(connection, dbplyr::in_schema(cdmDatabaseSchema, "person"))

  # get first n persons
  pairedSourcePersonAndPersonIds <- personTable |>
    dplyr::arrange(person_id) |>
    dplyr::select(person_id, person_source_value) |>
    dplyr::collect(n = numberPersons)


  return(pairedSourcePersonAndPersonIds)
}
