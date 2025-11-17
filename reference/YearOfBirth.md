# YearOfBirth

YearOfBirth

## Usage

``` r
YearOfBirth(
  connection,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cdmDatabaseSchema,
  cdmVersion = "5",
  cohortTable = "#cohort_person",
  cohortIds = c(-1),
  rowIdField = "subject_id",
  covariateSettings,
  aggregated = FALSE,
  minCharacterizationMean = 0
)
```

## Arguments

- connection:

  A database connection object created using
  [`DatabaseConnector::connect`](https://ohdsi.github.io/DatabaseConnector/reference/connect.html).

- tempEmulationSchema:

  The temp schema where the covariate tables will be created.

- cdmDatabaseSchema:

  The schema where the cdm tables are located.

- cdmVersion:

  The version of the cdm.

- cohortTable:

  The table where the cohort data is located.

- cohortIds:

  The cohort ids to include.

- rowIdField:

  The field in the cohort table that is the row id.

- covariateSettings:

  A list of settings for the covariate data.

- aggregated:

  Logical. If TRUE, the covariate data is aggregated.

- minCharacterizationMean:

  The minimum mean for the covariate to be included.
