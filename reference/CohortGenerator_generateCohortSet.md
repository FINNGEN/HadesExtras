# Wrap around CohortGenerator::generateCohortSet

Wrap around CohortGenerator::generateCohortSet to be able to build
cohorts based on cohortData. Checks if cohortDefinitionSet contais a
cohort based on cohortData. If so, uploads cohortData found in the json
column to the database and transforms that data in to a temporal table
with cohortTable format. CohortGenerator::generateCohortSet is called
and processes the sql column that will copy the contents of the temporal
cohortTable to the target cohortTable. It adds a column to the results
tibble, 'buildInfo' with a LogTibble with information on the cohort
construction.

## Usage

``` r
CohortGenerator_generateCohortSet(
  connectionDetails = NULL,
  connection = NULL,
  cdmDatabaseSchema,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cohortDatabaseSchema = cdmDatabaseSchema,
  cohortTableNames = CohortGenerator::getCohortTableNames(),
  cohortDefinitionSet = NULL,
  stopOnError = TRUE,
  incremental = FALSE,
  incrementalFolder = NULL
)
```

## Arguments

- connectionDetails:

  as in CohortGenerator::generateCohortSet.

- connection:

  as in CohortGenerator::generateCohortSet.

- cdmDatabaseSchema:

  as in CohortGenerator::generateCohortSet.

- tempEmulationSchema:

  as in CohortGenerator::generateCohortSet.

- cohortDatabaseSchema:

  as in CohortGenerator::generateCohortSet.

- cohortTableNames:

  as in CohortGenerator::generateCohortSet.

- cohortDefinitionSet:

  as in CohortGenerator::generateCohortSet.

- stopOnError:

  as in CohortGenerator::generateCohortSet.

- incremental:

  as in CohortGenerator::generateCohortSet.

- incrementalFolder:

  as in CohortGenerator::generateCohortSet.

## Value

results from CohortGenerator::generateCohortSet with additional column
'buildInfo'
