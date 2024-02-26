

test_that("createConnectionHandler works", {

  connectionHandler <- ResultModelManager_createConnectionHandler(
    connectionDetailsSettings = testSelectedConfiguration$connection$connectionDetailsSettings,
    tempEmulationSchema = testSelectedConfiguration$connection$tempEmulationSchema
  )

  CDMdb <- CDMdbHandler$new(
    databaseId = testSelectedConfiguration$database$databaseId,
    databaseName = testSelectedConfiguration$database$databaseName,
    databaseDescription = testSelectedConfiguration$database$databaseDescription,
    connectionHandler = connectionHandler,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    vocabularyDatabaseSchema = testSelectedConfiguration$cdm$vocabularyDatabaseSchema
  )

  on.exit({CDMdb$finalize()})

  CDMdb |> checkmate::expect_class("CDMdbHandler")
  CDMdb$databaseName |> checkmate::assertString()
  CDMdb$connectionStatusLog |> checkmate::expect_tibble()
  CDMdb$connectionStatusLog |> dplyr::filter(type != "SUCCESS") |> nrow() |>  expect_equal(0)
  CDMdb$getTblCDMSchema$person() |> checkmate::expect_class("tbl_dbi")
  CDMdb$getTblVocabularySchema$vocabulary() |> checkmate::expect_class("tbl_dbi")

})


test_that("createCDMdbHandlerFromList works with basicChecks", {

  CDMdb <-createCDMdbHandlerFromList(
    testSelectedConfiguration,
    loadConnectionChecksLevel = "basicChecks"
  )

  on.exit({CDMdb$finalize()})

  CDMdb |> checkmate::expect_class("CDMdbHandler")
  CDMdb$databaseName |> checkmate::assertString()
  CDMdb$connectionStatusLog |> checkmate::expect_tibble()
  CDMdb$connectionStatusLog |> dplyr::filter(type == "WARNING") |> nrow() |>  expect_equal(1)
  CDMdb$connectionStatusLog |> dplyr::filter(type == "WARNING") |> dplyr::pull(step) |> expect_equal("Check temp table creation")
  CDMdb$getTblCDMSchema$person() |> checkmate::expect_class("tbl_dbi")
  CDMdb$getTblVocabularySchema$vocabulary() |> checkmate::expect_class("tbl_dbi")

})
