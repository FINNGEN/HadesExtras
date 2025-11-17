# Create Temporal Covariate Settings From List

This function creates temporal covariate settings based on a list of
analysis IDs, temporal start days, and temporal end days.

## Usage

``` r
FeatureExtraction_createTemporalCovariateSettingsFromList(
  analysisIds,
  temporalStartDays = c(-99999),
  temporalEndDays = c(99999)
)
```

## Arguments

- analysisIds:

  A vector of analysis IDs.

- temporalStartDays:

  A vector of temporal start days for each analysis ID.

- temporalEndDays:

  A vector of temporal end days for each analysis ID.

## Value

A list of temporal covariate settings.
