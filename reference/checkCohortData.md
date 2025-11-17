# Check if a tibble is of cohortData format.

This function performs various validations on the cohortData tibble to
ensure its integrity and correctness. The validations include:

## Usage

``` r
checkCohortData(tibble)

assertCohortData(tibble)
```

## Arguments

- tibble:

  The tibble to be checked

## Value

TRUE if the tibble is of cohortData format, an array of strings with the
failed checks

## Details

- Checking if cohortData is a tibble.

- Checking if the cohortData tibble contains the required column names:
  cohort_name, person_source_value, cohort_start_date, cohort_end_date.

- Validating the types of cohort_name, person_source_value,
  cohort_start_date, and cohort_end_date columns (character and Date
  types, respectively).

- Checking for missing values in cohort_name and person_source_value
  columns.

- Verifying the order of cohort_start_date and cohort_end_date, ensuring
  cohort_start_date is older than cohort_end_date.
