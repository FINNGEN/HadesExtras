test_that("createConnectionHandler works", {
  config <- test_cohortTableHandlerConfig

  suppressWarnings({
    CDMdb <- createCDMdbHandlerFromList(config)
  })

  withr::defer({
    CDMdb$closeConnection()
    rm(CDMdb);gc()
  })

  CDMdb |> checkmate::expect_class("CDMdbHandler")
  CDMdb$databaseName |> checkmate::assertString()
  CDMdb$connectionStatusLog |> checkmate::expect_tibble()
  CDMdb$connectionStatusLog |>
    dplyr::slice(-5) |> # Remove the last 5 rows which are about resultsDatabaseSchema checks that are not relevant for this test
    dplyr::filter(type != "SUCCESS") |>
    nrow() |>
    expect_equal(0)
  CDMdb$getTblCDMSchema$person() |> checkmate::expect_class("tbl_dbi")
  CDMdb$getTblVocabularySchema$vocabulary() |> checkmate::expect_class("tbl_dbi")
})


test_that("createCDMdbHandlerFromList works with basicChecks", {
  config <- test_cohortTableHandlerConfig

  CDMdb <- createCDMdbHandlerFromList(
    config,
    loadConnectionChecksLevel = "basicChecks"
  )

  withr::defer({
    CDMdb$closeConnection()
    rm(CDMdb);gc()
  })

  CDMdb |> checkmate::expect_class("CDMdbHandler")
  CDMdb$databaseName |> checkmate::assertString()
  CDMdb$connectionStatusLog |> checkmate::expect_tibble()
  CDMdb$connectionStatusLog |>
    dplyr::slice(-5) |> # Remove the last 6 rows which are about resultsDatabaseSchema checks that are not relevant for this test
    dplyr::filter(type == "WARNING") |>
    nrow() |>
    expect_equal(1)
  CDMdb$connectionStatusLog |>
    dplyr::slice(-5) |> # Remove the last 6 rows which are about resultsDatabaseSchema checks that are not relevant for this test
    dplyr::filter(type == "WARNING") |>
    dplyr::pull(step) |>
    expect_equal("Check temp table creation")
  CDMdb$getTblCDMSchema$person() |> checkmate::expect_class("tbl_dbi")
  CDMdb$getTblVocabularySchema$vocabulary() |> checkmate::expect_class("tbl_dbi")
})


test_that("CDMdbHandler includes resultsDatabaseSchema", {
  config <- test_cohortTableHandlerConfig
  
  CDMdb <- createCDMdbHandlerFromList(
    config,
    loadConnectionChecksLevel = "allChecks"
  )
  
  withr::defer({
    CDMdb$closeConnection()
    rm(CDMdb)
    gc()
  })

  # Verify no errors in connection status log
  CDMdb$connectionStatusLog |>
    dplyr::filter(type == "ERROR") |>
    nrow() |>
    expect_equal(0)

  if(Sys.getenv("HADESEXTAS_TESTING_ENVIRONMENT") |> stringr::str_starts("Eunomia")){
    CDMdb$connectionStatusLog |>
    dplyr::filter(type == "WARNING") |>
    nrow() |>
    expect_equal(1)
  }
})


test_that("CDMdbHandler resultsDatabaseSchema can be set to different value", {
  skip_if_not(Sys.getenv("HADESEXTAS_TESTING_ENVIRONMENT") |> stringr::str_starts("AtlasDevelopment"), "This test is for checking that resultsDatabaseSchema can be set to a different value, but in Eunomia it is set to the same as cdmDatabaseSchema, so skipping this test in Eunomia environment.")
  
  # Get the test config and modify it to include a custom resultsDatabaseSchema
  config <- test_cohortTableHandlerConfig
  config$cdm$resultsDatabaseSchema <- "wrong_schema"  # Explicitly set wrong one
  
  suppressWarnings({
    CDMdb <- createCDMdbHandlerFromList(config, loadConnectionChecksLevel = "allChecks")
  })

  withr::defer({
    CDMdb$closeConnection()
    rm(CDMdb)
    gc()
  })
  
  # Verify there is an error in connection
  CDMdb$connectionStatusLog |>
    dplyr::slice(-5) |> # Remove the last 6 rows which are about resultsDatabaseSchema checks that are not relevant for this test
    dplyr::filter(type == "ERROR") |>
    nrow() |>
    expect_equal(1)
})

