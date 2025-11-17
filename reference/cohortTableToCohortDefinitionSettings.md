# Convert Cohort Table to Cohort Definition Settings

Converts cohort table entries into cohort definition settings for
specified cohort IDs within a given database schema. This function is
useful for generating cohort definition settings from existing cohort
data.

## Usage

``` r
cohortTableToCohortDefinitionSettings(
  cohortDatabaseSchema,
  cohortTable = "cohort",
  cohortDefinitionTable,
  cohortDefinitionIds,
  newCohortDefinitionIds = cohortDefinitionIds
)
```

## Arguments

- cohortDatabaseSchema:

  The database schema where the cohort table is located.

- cohortTable:

  The name of the cohort table. Defaults to "cohort".

- cohortDefinitionTable:

  The name of the cohort definition table where settings will be stored.

- cohortDefinitionIds:

  Numeric vector specifying cohort IDs to generate settings for.

- newCohortDefinitionIds:

  Numeric vector specifying new cohort IDs to assign to the settings.

## Value

A tibble containing cohort definition settings.
