
# splitString

testthat::test_that("test that splitString works with correct inputs", {
  splitString("2Upd3") |>
    expect_equal( c("2", "Upd", "3"))
  splitString("2 Upd 3") |>
    expect_equal( c("2", "Upd", "3"))
  splitString("2Upd3Ip4") |>
    expect_equal( c("2", "Upd", "3", "Ip", "4"))
  splitString("2 Upd 3 Ip 4") |>
    expect_equal( c("2", "Upd", "3", "Ip", "4"))
  splitString("2 Upd 3 Ip 4 Mp 5") |>
    expect_equal( c("2", "Upd", "3", "Ip", "4", "Mp", "5"))
})


testthat::test_that("test that splitString works with parenthesis", {
  splitString("(2Upd3)") |>
    expect_equal("(2Upd3)")
  splitString("2Upd(3Ip4)") |>
    expect_equal( c("2", "Upd", "(3Ip4)"))
  splitString("(2Upd3)Ip4") |>
    expect_equal( c("(2Upd3)", "Ip", "4"))
  splitString("(2Upd3)Ip(4Mp5)") |>
    expect_equal( c("(2Upd3)", "Ip", "(4Mp5)"))
  splitString("(2Upd3)Ip(4Mp5)Ip6") |>
    expect_equal( c("(2Upd3)", "Ip", "(4Mp5)", "Ip", "6"))
  splitString("2Upd(3Ip(4Mp5))") |>
    expect_equal( c("2", "Upd", "(3Ip(4Mp5))"))
  splitString("2Upd(3Ip(4Mp5)Ip6)") |>
    expect_equal( c("2", "Upd", "(3Ip(4Mp5)Ip6)"))
  splitString("2Upd(3Ip(4Mp5)Ip6)Ip7") |>
    expect_equal( c("2", "Upd", "(3Ip(4Mp5)Ip6)", "Ip", "7"))
  splitString("2Upd(3Ip(4Mp5)Ip6)Ip(7Mp8)") |>
    expect_equal( c("2", "Upd", "(3Ip(4Mp5)Ip6)", "Ip", "(7Mp8)"))
  splitString("2Upd(3Ip(4Mp5)Ip6)Ip(7Mp8)Ip9") |>
    expect_equal( c("2", "Upd", "(3Ip(4Mp5)Ip6)", "Ip", "(7Mp8)", "Ip", "9"))
  splitString("2Upd(3Ip(4Mp5)Ip6)Ip(7Mp8)Ip(9Mp10)") |>
    expect_equal( c("2", "Upd", "(3Ip(4Mp5)Ip6)", "Ip", "(7Mp8)", "Ip", "(9Mp10)"))
})

testthat::test_that("test that splitString errors", {
  splitString("2aaa3") |>
    expect_error('Invalid tokens: aaa')
  splitString("23sss(ddd222") |>
    expect_error('Invalid tokens: sss, ddd')
  splitString("2Upd(3Ip(4Mp5)Ip6") |>
    expect_error(' Missing closing brackets')
  splitString("2Upd(3Ip(4Mp5)Ip6Ip7") |>
    expect_error(' Missing closing brackets')
  splitString("2Upd(3Ip(4Mp5)Ip6Ip(7Mp8") |>
    expect_error(' Missing closing brackets')
  })


# .operationStringToBinaryTree

testthat::test_that("test that .operationStringToBinaryTreeworks", {
  .operationStringToBinaryTree("2Upd3") |>
    expect_equal(list( left=2, operation = "Upd",right=3))
  .operationStringToBinaryTree("(2Upd3)") |>
    expect_equal(list( left=2, operation = "Upd",right=3))
  .operationStringToBinaryTree("2Upd(3Ip4)") |>
    expect_equal(list( left=2, operation = "Upd",right=list( left=3, operation = "Ip",right=4)))
  .operationStringToBinaryTree("(2Upd3)Ip4")   |>
    expect_equal(list( left=list( left=2, operation = "Upd",right=3), operation = "Ip",right=4))
  .operationStringToBinaryTree("(2Upd3)Ip(4Mp5)") |>
    expect_equal(list( left=list( left=2, operation = "Upd",right=3), operation = "Ip",right=list( left=4, operation = "Mp",right=5)))
  .operationStringToBinaryTree("2Upd(3Ip(4Mp5))") |>
      expect_equal(list( left=2, operation = "Upd",right=list( left=3, operation = "Ip",right=list( left=4, operation = "Mp",right=5))))
  .operationStringToBinaryTree("2Upd(3Ip(4Mp5)Ip6)") |>
    expect_equal(list( left=2, operation = "Upd",right=list( left=3, operation = "Ip",right=list( left=list( left=4, operation = "Mp",right=5), operation = "Ip",right=6))))
  .operationStringToBinaryTree("(3Ip6)Ip(4Mp5)") |>
    expect_equal(list( left=list( left=3, operation = "Ip",right=6), operation = "Ip",right=list( left=4, operation = "Mp",right=5)))
  .operationStringToBinaryTree("2Upd(3Ip6)Ip(4Mp5)") |>
    expect_equal(list(left=2, operation = "Upd",right = list( left=list( left=3, operation = "Ip",right=6), operation = "Ip",right=list( left=4, operation = "Mp",right=5))))

  # priority
  .operationStringToBinaryTree("2Upd3Ip4") |>
    expect_equal(list( left=2, operation = "Upd",right=list( left=3, operation = "Ip",right=4)))
  .operationStringToBinaryTree("2Ip3Upd4") |>
    expect_equal(list( left=2, operation = "Ip",right=list( left=3, operation = "Upd",right=4)))
  .operationStringToBinaryTree("2Upd3Mp4") |>
    expect_equal(list( left=2, operation = "Upd",right=list( left=3, operation = "Mp",right=4)))
  .operationStringToBinaryTree("2Mp3Upd4") |>
    expect_equal(list( left=2, operation = "Mp",right=list( left=3, operation = "Upd",right=4)))
  .operationStringToBinaryTree("2Ip3Mp4") |>
    expect_equal(list( left=2, operation = "Ip",right=list( left=3, operation = "Mp",right=4)))
  .operationStringToBinaryTree("2Mp3Ip4") |>
    expect_equal(list( left=2, operation = "Mp",right=list( left=3, operation = "Ip",right=4)))
  .operationStringToBinaryTree("2Mp3Ip4Upd5") |>
    expect_equal(list( left=2, operation = "Mp",right=list( left=3, operation = "Ip",right=list( left=4, operation = "Upd",right=5))))

  })


# operationStringToSQL
# "1Ip2"
testthat::test_that("test that operationStringToSQL works", {

  connection <- helper_createNewConnection()
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  # set data
  testTable <- tibble::tibble(
    cohort_definition_id = c(1, 1, 1, 1,  2, 2, 2, 2),
    subject_id = c(1, 2, 3, 4,  3, 4, 5, 6),
    cohort_start_date = c(rep(as.Date("2000-01-01"), 4), rep(as.Date("2010-01-01"), 4)),
    cohort_end_date = c(rep(as.Date("2000-12-01"), 4), rep(as.Date("2010-12-01"), 4))
  )

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    table = "testOperationsTable",
    data = testTable
  )

  # function
  sql <- operationStringToSQL("1Ip2")

  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohort_table = "testOperationsTable",
    output_table = "",
    warnOnMissingParameters = TRUE
  )

  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms
  )

  result <- DatabaseConnector::dbGetQuery(connection, sql, progressBar = FALSE, reportOverallTime = FALSE) |> tibble::as_tibble()

  # results
  result |> dplyr::pull(subject_id) |> expect_equal(c( 3, 4))
  result |> dplyr::pull(cohort_start_date) |> expect_equal(c( as.Date("2000-01-01"), as.Date("2000-01-01")))
  result |> dplyr::pull(cohort_end_date) |> expect_equal(c( as.Date("2000-12-01"), as.Date("2000-12-01")))


})


# "1Mp2"
testthat::test_that("test that operationStringToSQL works", {

  connection <- helper_createNewConnection()
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  # set data
  testTable <- tibble::tibble(
    cohort_definition_id = c(1, 1, 1, 1,  2, 2, 2, 2),
    subject_id = c(1, 2, 3, 4,  3, 4, 5, 6),
    cohort_start_date = c(rep(as.Date("2000-01-01"), 4), rep(as.Date("2010-01-01"), 4)),
    cohort_end_date = c(rep(as.Date("2000-12-01"), 4), rep(as.Date("2010-12-01"), 4))
  )

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    table = "testOperationsTable",
    data = testTable
  )

  # function
  sql <- operationStringToSQL("1Mp2")

  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohort_table = "testOperationsTable",
    output_table = "",
    warnOnMissingParameters = TRUE
  )

  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms
  )

  result <- DatabaseConnector::dbGetQuery(connection, sql, progressBar = FALSE, reportOverallTime = FALSE) |> tibble::as_tibble()

  # results
  result |> dplyr::pull(subject_id) |> expect_equal(c( 1, 2))
  result |> dplyr::pull(cohort_start_date) |> expect_equal(c( as.Date("2000-01-01"), as.Date("2000-01-01")))
  result |> dplyr::pull(cohort_end_date) |> expect_equal(c( as.Date("2000-12-01"), as.Date("2000-12-01")))


})




# "1Ip2Mp3"
testthat::test_that("test that operationStringToSQL works", {

  connection <- helper_createNewConnection()
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  # set data
  testTable <- tibble::tibble(
    cohort_definition_id = c(1, 1, 1, 1,  2, 2, 2, 2, 3, 3, 3, 3),
    subject_id = c(1, 2, 3, 4,  3, 4, 5, 6,  4, 10, 11, 12),
    cohort_start_date = c(rep(as.Date("2000-01-01"), 4), rep(as.Date("2010-01-01"), 4), rep(as.Date("2020-01-01"), 4)),
    cohort_end_date = c(rep(as.Date("2000-12-01"), 4), rep(as.Date("2010-12-01"), 4), rep(as.Date("2020-12-01"), 4))
  )

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    table = "testOperationsTable",
    data = testTable
  )

  # function
  sql <- operationStringToSQL("1Ip2Mp3")

  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohort_table = "testOperationsTable",
    output_table = "",
    warnOnMissingParameters = TRUE
  )

  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms
  )

  result <- DatabaseConnector::dbGetQuery(connection, sql, progressBar = FALSE, reportOverallTime = FALSE) |> tibble::as_tibble()

  # results
  result |> dplyr::pull(subject_id) |> expect_equal(c( 3))
  result |> dplyr::pull(cohort_start_date) |> expect_equal(c( as.Date("2000-01-01")))
  result |> dplyr::pull(cohort_end_date) |> expect_equal(c( as.Date("2000-12-01")))


})


# "1Upd2"
testthat::test_that("test that operationStringToSQL works", {

  connection <- helper_createNewConnection()
  on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  # set data
  testTable <-  tibble::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1, 1, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 2, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 3, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 4, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2000-01-01"), as.Date("2000-12-01"),
    1, 5, as.Date("2004-01-01"), as.Date("2004-12-01"),

    2, 2, as.Date("2001-01-01"), as.Date("2002-12-01"),# non overplaping
    2, 3, as.Date("2000-06-01"), as.Date("2000-09-01"),# inside
    2, 4, as.Date("2000-06-01"), as.Date("2010-12-01"),# overlap
    2, 5, as.Date("2004-06-01"), as.Date("2010-12-01"),# overlap with second
    2, 6, as.Date("2000-01-01"), as.Date("2010-12-01")
  )


  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    table = "testOperationsTable",
    data = testTable
  )

  # function
  sql <- operationStringToSQL("1Upd2")

  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohort_table = "testOperationsTable",
    output_table = "",
    warnOnMissingParameters = TRUE
  )

  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms
  )

  result <- DatabaseConnector::dbGetQuery(connection, sql, progressBar = FALSE, reportOverallTime = FALSE) |> tibble::as_tibble()

  # results
  result |> dplyr::pull(subject_id) |> expect_equal(c( 1, 2, 3, 4, 5, 6))
  result |> dplyr::pull(cohort_start_date) |> expect_equal(rep(as.Date("2000-01-01"), 6))
  result |> dplyr::pull(cohort_end_date) |> expect_equal(as.Date(c("2000-12-01", "2002-12-01", "2000-12-01", "2010-12-01", "2010-12-01", "2010-12-01")))


})
























