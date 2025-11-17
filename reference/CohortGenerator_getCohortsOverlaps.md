# CohortGenerator_getCohortsOverlaps

Description: A function to calculate overlaps between cohorts.

## Usage

``` r
CohortGenerator_getCohortsOverlaps(
  connectionDetails = NULL,
  connection = NULL,
  cohortDatabaseSchema,
  cohortTable = "cohort",
  cohortIds = c()
)
```

## Arguments

- connectionDetails:

  A list containing details for connecting to the database. Default is
  NULL.

- connection:

  A database connection object. Default is NULL.

- cohortDatabaseSchema:

  The schema where the cohort table resides.

- cohortTable:

  The name of the cohort table.

- cohortIds:

  A vector of cohort ids for which to calculate overlaps. Default is an
  empty vector.

## Value

A tibble containing the overlaps between cohorts. Column
`cohortIdCombinations` indicates the cohort ids separated by - in the
combination `numberOfSubjects` with the number of subjects in the
combination.
