# Get Cohort Names from Cohort Definition Table

Retrieves cohort names along with their corresponding IDs and
descriptions from the specified cohort definition table within a given
database schema. This function assists in querying cohort information
from the database.

## Usage

``` r
getCohortNamesFromCohortDefinitionTable(
  connectionDetails = NULL,
  connection = NULL,
  cohortDatabaseSchema,
  cohortDefinitionTable = "cohort_definition"
)
```

## Arguments

- connectionDetails:

  Details required to establish a database connection (optional).

- connection:

  An existing database connection (optional).

- cohortDatabaseSchema:

  The database schema where the cohort definition table is expected to
  be found.

- cohortDefinitionTable:

  The name of the cohort definition table. Defaults to
  "cohort_definition".

## Value

A tibble containing cohort names, IDs, and descriptions.
