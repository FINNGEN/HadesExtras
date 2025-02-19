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


helper_FinnGen_getDatabaseFile <- function(){
   if( Sys.getenv("EUNOMIA_DATA_FOLDER") == "" ){
    message("EUNOMIA_DATA_FOLDER not set. Please set this environment variable to the path of the Eunomia data folder.")
    stop()
  }

  urlToFinnGenEunomiaZip <- "https://raw.githubusercontent.com/FINNGEN/EunomiaDatasets/main/datasets/FinnGenR12/FinnGenR12_v5.4.zip"
  eunomiaDataFolder <- Sys.getenv("EUNOMIA_DATA_FOLDER")

  # Download the database if it doesn't exist
  if (!file.exists(file.path(eunomiaDataFolder, "FinnGenR12_v5.4.zip")) | !file.exists(file.path(eunomiaDataFolder, "FinnGenR12_v5.4.sqlite"))){

    result <- utils::download.file(
      url = urlToFinnGenEunomiaZip,
      destfile = file.path(eunomiaDataFolder, "FinnGenR12_v5.4.zip"),
      mode = "wb"
    )

    Eunomia::extractLoadData(
      from = file.path(eunomiaDataFolder, "FinnGenR12_v5.4.zip"),
      to = file.path(eunomiaDataFolder, "FinnGenR12_v5.4.sqlite"),
      cdmVersion = '5.4',
      verbose = TRUE
    )
  }

  # copy to a temp folder
  file.copy(
    from = file.path(eunomiaDataFolder, "FinnGenR12_v5.4.sqlite"),
    to = file.path(tempdir(), "FinnGenR12_v5.4.sqlite"),
    overwrite = TRUE
  )

  return(file.path(tempdir(), "FinnGenR12_v5.4.sqlite"))
}
