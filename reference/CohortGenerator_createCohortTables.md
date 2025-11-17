# CohortGenerator_createCohortTables

Wrapper for CohortGenerator::createCohortTables, where if bigquery is
used, the tables are created using bigrquery::bq_table_create.

## Usage

``` r
CohortGenerator_createCohortTables(
  connectionDetails = NULL,
  connection = NULL,
  cohortDatabaseSchema,
  cohortTableNames = CohortGenerator::getCohortTableNames(),
  incremental = FALSE
)
```

## Arguments

- connectionDetails:

  An object of class `connectionDetails` containing database connection
  details.

- connection:

  A database connection object created using
  [`DatabaseConnector::connect`](https://ohdsi.github.io/DatabaseConnector/reference/connect.html).

- cohortDatabaseSchema:

  The schema where the cohort tables will be created.

- cohortTableNames:

  A list containing the names of the cohort tables to be created.

- incremental:

  Logical. If TRUE, only creates tables that don't already exist.
