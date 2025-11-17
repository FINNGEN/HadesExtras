# CohortGenerator_dropCohortStatsTables

Wrapper for CohortGenerator::dropCohortStatsTables, where if bigquery is
used, the tables are deleted using bigrquery::bq_table_delete.

## Usage

``` r
CohortGenerator_dropCohortStatsTables(
  connectionDetails = NULL,
  connection = NULL,
  cohortDatabaseSchema,
  cohortTableNames = CohortGenerator::getCohortTableNames()
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
