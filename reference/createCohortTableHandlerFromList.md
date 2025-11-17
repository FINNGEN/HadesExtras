# createCohortTableHandlerFromList

A function to create a CohortTableHandler object from a list of
cohortTableHandlerConfiguration settings.

## Usage

``` r
createCohortTableHandlerFromList(
  cohortTableHandlerConfig,
  loadConnectionChecksLevel = "allChecks"
)
```

## Arguments

- cohortTableHandlerConfig:

  A list containing cohortTableHandlerConfiguration settings for the
  CohortTableHandler.

  - databaseName: The name of the database.

  - connection: A list of connection details settings.

  - cdm: A list of CDM database schema settings.

  - cohortTable: A list of cohort table settings.

- loadConnectionChecksLevel:

  (Optional) Level of checks to perform when loading the connection
  (default is "allChecks").

## Value

A CohortTableHandler object.
