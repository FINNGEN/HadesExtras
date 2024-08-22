
helper_createNewConnection <- function(addCohorts = FALSE){
    connectionDetailsSettings <- testSelectedConfiguration$connection$connectionDetailsSettings

  if(connectionDetailsSettings$dbms == "eunomia"){
    connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  }else{
    connectionDetails <- rlang::exec(DatabaseConnector::createConnectionDetails, !!!connectionDetailsSettings)
  }

  # set tempEmulationSchema if in config
  if(!is.null(testSelectedConfiguration$connection$tempEmulationSchema)){
    options(sqlRenderTempEmulationSchema = testSelectedConfiguration$connection$tempEmulationSchema)
  }else{
    options(sqlRenderTempEmulationSchema = NULL)
  }

  # set useBigrqueryUpload if in config
  if(!is.null(testSelectedConfiguration$connection$useBigrqueryUpload)){
    options(useBigrqueryUpload = testSelectedConfiguration$connection$useBigrqueryUpload)

    # bq authentication
    if(testSelectedConfiguration$connection$useBigrqueryUpload==TRUE){
      checkmate::assertTRUE(connectionDetails$dbms=="bigquery")

      options(gargle_oauth_cache=FALSE) #to avoid the question that freezes the app
      connectionString <- connectionDetails$connectionString()
      if( connectionString |> stringr::str_detect(";OAuthType=0;")){
        OAuthPvtKeyPath <- connectionString |>
          stringr::str_extract("OAuthPvtKeyPath=([:graph:][^;]+);") |>
          stringr::str_remove("OAuthPvtKeyPath=") |> stringr::str_remove(";")

        checkmate::assertFileExists(OAuthPvtKeyPath)
        bigrquery::bq_auth(path = OAuthPvtKeyPath)

      }else{
        bigrquery::bq_auth(scopes = "https://www.googleapis.com/auth/bigquery")
      }

      connectionDetails$connectionString
    }

  }else{
    options(useBigrqueryUpload = NULL)
  }

  if(addCohorts){
    Eunomia::createCohorts(connectionDetails)
  }

  connection <- DatabaseConnector::connect(connectionDetails)

  return(connection)
}


helper_getParedSourcePersonAndPersonIds  <- function(
    connection,
    cohortDatabaseSchema,
    numberPersons){

  # Connect, collect tables
  personTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, "person"))

  # get first n persons
  pairedSourcePersonAndPersonIds  <- personTable  |>
    dplyr::arrange(person_id) |>
    dplyr::select(person_id, person_source_value) |>
    dplyr::collect(n=numberPersons)


  return(pairedSourcePersonAndPersonIds)
}




helper_createNewCohortTableHandler <- function(addCohorts = NULL){

  addCohorts |> checkmate::assertCharacter(len = 1, null.ok = TRUE)
  addCohorts |> checkmate::assertSubset(c("EunomiaDefaultCohorts", "HadesExtrasFractureCohorts"), empty.ok = TRUE)

  cohortTableHandlerConfig <- cohortTableHandlerConfig # set by setup.R
  loadConnectionChecksLevel = "basicChecks"

  cohortTableHandler <- HadesExtras::createCohortTableHandlerFromList(cohortTableHandlerConfig, loadConnectionChecksLevel)

  if(!is.null(addCohorts) ){
    if(addCohorts == "EunomiaDefaultCohorts"){
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
    if(addCohorts == "HadesExtrasFractureCohorts"){
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
    cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
  }

  return(cohortTableHandler)
}
