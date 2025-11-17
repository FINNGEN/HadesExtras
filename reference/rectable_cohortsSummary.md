# Cohorts Summary Table

Generates a summary table of cohorts, including various cohort
statistics.

## Usage

``` r
rectable_cohortsSummary(
  cohortsSummary,
  deleteButtonsShinyId = NULL,
  editButtonsShinyId = NULL
)
```

## Arguments

- cohortsSummary:

  A tibble in cohortsSummary format.

- deleteButtonsShinyId:

  An optional Shiny input ID for handling the click of the delete
  buttons.

- editButtonsShinyId:

  An optional Shiny input ID for handling the click of the edit buttons.

## Value

A reactable table displaying cohort summary information.
