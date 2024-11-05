



table <- dbplyr::in_schema("atlas-development-270609.finngen_omop_r11", "person")
connection <- CDMdb$connectionHandler$getConnection()
dplyr::tbl(connection, table)


bigrquery::bq_auth(path = Sys.getenv("GCP_SERVICE_KEY"))

con <- DBI::dbConnect(
  bigrquery::bigquery(),
  project = "atlas-development-270609",
  billing =  'atlas-development-270609'
)
con

dplyr::tbl(con, table)  |> dplyr::show_query()







bigrquery::bq_auth(path = Sys.getenv("GCP_SERVICE_KEY"))

con <- DBI::dbConnect(
  bigrquery::bigquery(),
  project = "atlas-development-270609",
  billing =  'atlas-development-270609'
)

cd <- DatabaseConnector::createDbiConnectionDetails(
  dbms = "bigquery",
  drv = bigrquery::bigquery(),
  project = "atlas-development-270609",
  billing =  'atlas-development-270609'
)

connection2 <- DatabaseConnector::connect(cd)

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
