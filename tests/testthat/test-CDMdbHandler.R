test_that("createConnectionHandler works", {

  config <- test_cohortTableHandlerConfig

  CDMdb <- createCDMdbHandlerFromList(config)

  withr::defer({
    CDMdb$finalize()
  })

  CDMdb |> checkmate::expect_class("CDMdbHandler")
  CDMdb$databaseName |> checkmate::assertString()
  CDMdb$connectionStatusLog |> checkmate::expect_tibble()
  CDMdb$connectionStatusLog |>
    dplyr::filter(type != "SUCCESS") |>
    nrow() |>
    expect_equal(0)
  CDMdb$getTblCDMSchema$person() |> checkmate::expect_class("tbl_dbi")
  CDMdb$getTblVocabularySchema$vocabulary() |> checkmate::expect_class("tbl_dbi")



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
  useBigrqueryUpload: true #optional
cdm:
  cdmDatabaseSchema: atlas-development-270609.finngen_omop_r11
  vocabularyDatabaseSchema: atlas-development-270609.finngen_omop_r11
cohortTable:
  cohortDatabaseSchema: atlas-development-270609.sandbox
  cohortTableName: test_cohort_table
'

config <- yaml::read_yaml(text = config)
connectionHandler <- ResultModelManager_createConnectionHandler(
    connectionDetailsSettings = config$connection$connectionDetailsSettings,
    tempEmulationSchema = config$connection$tempEmulationSchema
  )

connectionHandler$getConnection()
connectionHandler$tbl('person', config$cdm$cdmDatabaseSchema )

})


test_that("createCDMdbHandlerFromList works with basicChecks", {
  
  config <- test_cohortTableHandlerConfig

  CDMdb <- createCDMdbHandlerFromList(
    config,
    loadConnectionChecksLevel = "basicChecks"
  )

  withr::defer({
    CDMdb$finalize()
  })

  CDMdb |> checkmate::expect_class("CDMdbHandler")
  CDMdb$databaseName |> checkmate::assertString()
  CDMdb$connectionStatusLog |> checkmate::expect_tibble()
  CDMdb$connectionStatusLog |>
    dplyr::filter(type == "WARNING") |>
    nrow() |>
    expect_equal(1)
  CDMdb$connectionStatusLog |>
    dplyr::filter(type == "WARNING") |>
    dplyr::pull(step) |>
    expect_equal("Check temp table creation")
  CDMdb$getTblCDMSchema$person() |> checkmate::expect_class("tbl_dbi")
  CDMdb$getTblVocabularySchema$vocabulary() |> checkmate::expect_class("tbl_dbi")
})
