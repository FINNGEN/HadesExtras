test_databasesConfig <- readAndParseYaml(
    pathToYalmFile = testthat::test_path("config", "atlasDev_databasesConfig.yml"),
    OAuthPvtKeyPath = Sys.getenv("GCP_SERVICE_KEY"),
    pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  )

connectionDetails <- rlang::exec(DatabaseConnector::createConnectionDetails, !!!test_databasesConfig$BQ5K$cohortTableHandler$connection$connectionDetailsSettings)
connection_jdbi <- DatabaseConnector::connect(connectionDetails)

cars <- dplyr::copy_to(connection_jdbi, cars, overwrite = TRUE)



bigrquery::bq_auth(path = Sys.getenv("GCP_SERVICE_KEY"))

connection_bigrquery <- DBI::dbConnect(
  bigrquery::bigquery(),
  project = "atlas-development-270609",
  billing =  'atlas-development-270609'
)


cars <- dplyr::copy_to(connection_bigrquery, cars, overwrite = TRUE)




bigrquery::bq_auth(path = Sys.getenv("GCP_SERVICE_KEY"))
options(sqlRenderTempEmulationSchema = "atlas-development-270609.sandbox")

cd <- DatabaseConnector::createDbiConnectionDetails(
  dbms = "bigquery",
  drv = bigrquery::bigquery(),
  project = "atlas-development-270609",
  billing =  'atlas-development-270609'
)

connection_dbi <- DatabaseConnector::connect(cd)

aaa <- mtcars |> tibble::as_tibble() |> dplyr::mutate(speed = as.numeric(NA)) 
tbl_cars <- dplyr::copy_to(connection_dbi, aaa , overwrite = TRUE)




bq_table <- bigrquery::bq_table("atlas-development-270609", "sandbox", "aaaaa")
bigrquery::bq_table_upload(bq_table, values = aaa, fields = bigrquery::bq_fields(aaa))

bigrquery::bq_table_download(bq_table)




table <- dbplyr::in_schema("atlas-development-270609.finngen_omop_r11", "person")
table <- dbplyr::in_schema("atlas-development-270609.finngen_omop_r11", "person")
dplyr::tbl(connection, table)


dplyr::copy_to(connection, cars,  overwrite = TRUE)

DatabaseConnector::insertTable(
  connection = connection2,
  tableName = "cars",
  data = cars,
  tempTable = TRUE
)


# would need chage in
# https://github.com/OHDSI/DatabaseConnector/blob/46eb774beae80714efa7a6ad39558c478ceb597c/R/InsertTable.R#L391
# to hadel bigrquery as in default



table <- dbplyr::in_schema("atlas-development-270609.finngen_omop_r11", "person")
connection <- CDMdb$connectionHandler$getConnection()
dplyr::tbl(connection, table)

config <- '
database:
  databaseId: BQ1
  databaseName: bigquery1
  databaseDescription: BigQuery database
connection:
  connectionDetailsSettings:
    dbms: bigquery
    user: ""
    password: ""
    connectionString: jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=atlas-development-270609;OAuthType=0;OAuthServiceAcctEmail=146473670970-compute@developer.gserviceaccount.com;OAuthPvtKeyPath=/Users/javier/keys/atlas-development-270609-410deaacc58b.json;Timeout=100000;
    pathToDriver: /Users/javier/.config/hades/bigquery
  tempEmulationSchema: atlas-development-270609.sandbox #optional
cdm:
  cdmDatabaseSchema: atlas-development-270609.finngen_omop_r11
  vocabularyDatabaseSchema: atlas-development-270609.finngen_omop_r11
cohortTable:
  cohortDatabaseSchema: atlas-development-270609.sandbox
  cohortTableName: test_cohort_table
'

config <- yaml::read_yaml(text = config)

connectionDetails <- rlang::exec(DatabaseConnector::createConnectionDetails, !!!config$connection$connectionDetailsSettings)
options(sqlRenderTempEmulationSchema = config$connection$tempEmulationSchema)
options(useBigrqueryUpload = NULL)
connectionHandler <- ResultModelManager::ConnectionHandler$new(connectionDetails, loadConnection = FALSE)

connectionHandler$getConnection()
connectionHandler$tbl('person', config$cdm$cdmDatabaseSchema )


sub(".*ProjectId=([^;]+).*", "\\1", "/www.googleapis.com/bigquery/v2:443;ProjectId=atlas-development-270609;OAuthType=0;OAuthServiceAcctEmail=146473670970-compute@developer.gserviceac")
