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

helper_createNewCohortTableHandler <- function(loadConnectionChecksLevel = "basicChecks") {

  # by default use the one from setup.R
  cohortTableHandlerConfig <- test_cohortTableHandlerConfig # set by setup.R

  cohortTableHandler <- createCohortTableHandlerFromList(cohortTableHandlerConfig, loadConnectionChecksLevel)

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
