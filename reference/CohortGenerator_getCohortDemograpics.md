# Get cohort counts and basic cohort demographics for a specific cohort.

This function retrieves demographic information for a given cohort .

## Usage

``` r
CohortGenerator_getCohortDemograpics(
  connectionDetails = NULL,
  connection = NULL,
  cdmDatabaseSchema,
  vocabularyDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema,
  cohortTable = "cohort",
  cohortIds = c(),
  toGet = c("histogramCohortStartYear", "histogramCohortEndYear", "histogramBirthYear",
    "histogramBirthYearAllEvents", "sexCounts", "sexCountsAllEvents"),
  cohortDefinitionSet = NULL,
  databaseId = NULL
)
```

## Arguments

- connectionDetails:

  Details required to establish a database connection (optional).

- connection:

  An existing database connection (optional).

- cdmDatabaseSchema:

  The schema name for the Common Data Model (CDM) database.

- vocabularyDatabaseSchema:

  The schema name for the vocabulary database (default is
  `cdmDatabaseSchema`).

- cohortDatabaseSchema:

  The schema name for the cohort database.

- cohortTable:

  The name of the cohort table in the cohort database (default is
  "cohort").

- cohortIds:

  A numeric vector of cohort IDs for which to retrieve demographics
  (default is an empty vector).

- toGet:

  A character vector indicating which demographic information to
  retrieve. Possible values include "histogramCohortStartYear",
  "histogramCohortEndYear", "histogramBirthYear",
  "histogramBirthYearAllEvents", "sexCounts", and "sexCountsAllEvents".

- cohortDefinitionSet:

  A set of cohort definitions (optional).

- databaseId:

  The ID of the database (optional).

## Value

A data frame with cohort counts and selected demographics
