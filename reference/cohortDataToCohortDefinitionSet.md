# cohortDataToCohortDefinitionSet

Convert a cohortData tibble into a cohortDefinitionSet

## Usage

``` r
cohortDataToCohortDefinitionSet(
  cohortData,
  newCohortIds = NULL,
  skipCohortDataCheck = FALSE
)
```

## Arguments

- cohortData:

  The tibble in cohortData format.

- newCohortIds:

  A vector of new cohort IDs to be assigned to the cohort data.

- skipCohortDataCheck:

  Logical value indicating whether to skip cohort data validation
  (default is FALSE).

## Value

A data frame of format cohortDefinitionSet.
