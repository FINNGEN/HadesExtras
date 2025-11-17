# A definition of subset functions to be applied to a set of cohorts

A definition of subset functions to be applied to a set of cohorts

## Usage

``` r
createMatchingSubset(
  name = NULL,
  matchToCohortId,
  matchRatio = 10,
  matchSex = TRUE,
  matchBirthYear = TRUE,
  matchCohortStartDateWithInDuration = FALSE,
  newCohortStartDate = "keep",
  newCohortEndDate = "keep"
)
```

## Arguments

- name:

  optional name of operator

- matchToCohortId:

  integer - cohort ids to match to

- matchRatio:

  matching ratio

- matchSex:

  match to target sex

- matchBirthYear:

  match to target birth year

- matchCohortStartDateWithInDuration:

  cohort_start_date of the matching subject must be within the duration
  of the target cohort

- newCohortStartDate:

  change cohort start date to the matched person

- newCohortEndDate:

  change cohort end date to the matched person

## Value

a MatchingSubsetOperator instance
