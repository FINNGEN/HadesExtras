# Check Existence of Cohort Definition Tables

Validates the existence of the specified cohort definition and cohort
tables within a given database schema. Requires either an active
database connection or connection details to create one. This function
is intended to assist in ensuring that the necessary tables for cohort
definition are present in the database.

## Usage

``` r
checkCohortDefinitionTables(
  connectionDetails = NULL,
  connection = NULL,
  cohortDatabaseSchema,
  cohortDefinitionTable = "cohort_definition",
  cohortTable = "cohort"
)
```

## Arguments

- connectionDetails:

  Details required to establish a database connection (optional).

- connection:

  An existing database connection (optional).

- cohortDatabaseSchema:

  The database schema where the cohort definition tables are expected to
  be found.

- cohortDefinitionTable:

  The name of the cohort definition table. Defaults to
  "cohort_definition".

- cohortTable:

  The name of the table containing cohort data. Defaults to "cohort".

## Value

A tibble log indicating the success or error in checking the existence
of tables.
