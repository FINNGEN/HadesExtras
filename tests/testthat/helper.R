
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



helper_createNewCohortTableHandler <- function(){
  connectionHandler <- ResultModelManager_createConnectionHandler(
    connectionDetailsSettings = testSelectedConfiguration$connection$connectionDetailsSettings,
    tempEmulationSchema = testSelectedConfiguration$connection$tempEmulationSchema
  )

  cohortTableHandler <- CohortTableHandler$new(
    connectionHandler = connectionHandler,
    databseId = testSelectedConfiguration$database$databaseId,
    databaseName = testSelectedConfiguration$database$databaseName,
    databaseDescription = testSelectedConfiguration$database$databaseDescription,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    vocabularyDatabaseSchema = testSelectedConfiguration$cdm$vocabularyDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableName = testSelectedConfiguration$cohortTable$cohortTableName
  )
  return(cohortTableHandler)
}
