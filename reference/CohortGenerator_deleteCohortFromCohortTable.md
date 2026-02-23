# deleteCohortFromCohortTable

Deletes specified cohorts from a cohort table in a database.

## Usage

``` r
CohortGenerator_deleteCohortFromCohortTable(
  connectionDetails = NULL,
  connection = NULL,
  cohortDatabaseSchema,
  cohortTableNames,
  cohortIds
)
```

## Arguments

- connectionDetails:

  A list of database connection details (optional).

- connection:

  A pre-established database connection (optional).

- cohortDatabaseSchema:

  The schema name of the cohort database.

- cohortTableNames:

  A list containing the name of the cohort table.

- cohortIds:

  Numeric vector of cohort IDs to be deleted.

## Value

TRUE if the deletion was successful; otherwise, an error is raised.
