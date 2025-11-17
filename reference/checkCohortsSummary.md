# Check if a tibble is of CohortsSummary format.

This function performs various validations on the CohortsSummary tibble
to ensure its integrity and correctness. The validations include:

## Usage

``` r
checkCohortsSummary(tibble)

assertCohortsSummary(tibble)
```

## Arguments

- tibble:

  The tibble to be checked

## Value

TRUE if the tibble is of CohortsSummary format, an array of strings with
the failed checks

## Details

- Checking if CohortsSummary is a tibble.

- Checking if the CohortsSummary tibble contains the required column
  names

- Validating the types
