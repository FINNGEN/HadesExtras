# covariateData_ATCgroups

Creates covariate settings for ATC drug groups

## Usage

``` r
covariateData_ATCgroups(
  temporalStartDays = -99999,
  temporalEndDays = 99999,
  continuous = FALSE
)
```

## Arguments

- temporalStartDays:

  Start day relative to index (-99999 by default)

- temporalEndDays:

  End day relative to index (99999 by default)

- continuous:

  Logical. If TRUE, the covariate data is continuous.

## Value

A covariate settings object for ATC drug groups
