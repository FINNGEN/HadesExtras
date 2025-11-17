# Get Cohort Data from Cohort Table

This function retrieves cohort data from a cohort table in a database.

## Usage

``` r
getCohortDataFromCohortTable(
  connectionDetails = NULL,
  connection = NULL,
  cdmDatabaseSchema,
  cohortDatabaseSchema,
  cohortTable,
  cohortNameIds
)
```

## Arguments

- connectionDetails:

  A list of connection details (optional).

- connection:

  An existing database connection (optional).

- cdmDatabaseSchema:

  The schema name for the CDM database.

- cohortDatabaseSchema:

  The schema name for the cohort database.

- cohortTable:

  The name of the cohort table.

- cohortNameIds:

  A data frame containing cohort name IDs.

## Value

Returns TRUE if the cohort data is successfully retrieved.

## Details

This function retrieves cohort data from a specified cohort table in a
database. It validates the input parameters, establishes a database
connection if one is not provided, and then performs SQL operations to
retrieve the cohort data.
