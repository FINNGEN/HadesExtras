test_that("createConnectionHandler works", {
  config <- test_cohortTableHandlerConfig

  suppressWarnings({
    CDMdb <- createCDMdbHandlerFromList(config)
  })

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
