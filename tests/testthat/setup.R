#
# SELECT DATABASE and CO2 CONFIGURATION
#
testingDatabase <- "Eunomia-GiBleed"
# testingDatabase <- "Eunomia-MIMIC"
#testingDatabase <- "AtlasDevelopment"

# check correct settings
possibleDatabases <- c("Eunomia-GiBleed", "Eunomia-MIMIC", "AtlasDevelopment")
if (!(testingDatabase %in% possibleDatabases)) {
  message("Please select a valid database from: ", paste(possibleDatabases, collapse = ", "))
  stop()
}

#
# Eunomia Databases
#
if (testingDatabase |> stringr::str_starts("Eunomia")) {
  if (Sys.getenv("EUNOMIA_DATA_FOLDER") == "") {
    message("EUNOMIA_DATA_FOLDER not set. Please set this environment variable to the path of the Eunomia data folder.")
    stop()
  }

  pathToGiBleedEunomiaSqlite <- Eunomia::getDatabaseFile("GiBleed", overwrite = FALSE)
  pathToMIMICEunomiaSqlite <- Eunomia::getDatabaseFile("MIMIC", overwrite = FALSE)

  test_databasesConfig <- readAndParseYaml(
    pathToYalmFile = testthat::test_path("config", "eunomia_databasesConfig.yml"),
    pathToGiBleedEunomiaSqlite = pathToGiBleedEunomiaSqlite,
    pathToMIMICEunomiaSqlite = pathToMIMICEunomiaSqlite
  )

  if (testingDatabase |> stringr::str_ends("GiBleed")) {
    test_cohortTableHandlerConfig <- test_databasesConfig[[1]]$cohortTableHandle
  }
  if (testingDatabase |> stringr::str_ends("MIMIC")) {
    test_cohortTableHandlerConfig <- test_databasesConfig[[2]]$cohortTableHandle
  }
}


#
# AtlasDevelopmet Database
#
if (testingDatabase %in% c("AtlasDevelopment")) {
  if (Sys.getenv("GCP_SERVICE_KEY") == "") {
    message("GCP_SERVICE_KEY not set. Please set this environment variable to the path of the GCP service key.")
    stop()
  }

  if (Sys.getenv("DATABASECONNECTOR_JAR_FOLDER") == "") {
    message("DATABASECONNECTOR_JAR_FOLDER not set. Please set this environment variable to the path of the database connector jar folder.")
    stop()
  }

  test_databasesConfig <- readAndParseYaml(
    pathToYalmFile = testthat::test_path("config", "atlasDev_databasesConfig.yml"),
    OAuthPvtKeyPath = Sys.getenv("GCP_SERVICE_KEY"),
    pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  )

  test_cohortTableHandlerConfig <- test_databasesConfig[[1]]$cohortTableHandler
}


#
# INFORM USER
#
message("************* Testing on: ")
message("Database: ", testingDatabase)
