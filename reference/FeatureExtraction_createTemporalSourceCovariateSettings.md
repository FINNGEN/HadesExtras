# Create Temporal Source Covariate Settings

This function generates settings for temporal source covariates to be
used in feature extraction, DEPRECATED use
FeatureExtraction_createTemporalCovariateSettingsFromList.

## Usage

``` r
FeatureExtraction_createTemporalSourceCovariateSettings(
  useConditionOccurrenceSourceConcept = TRUE,
  useDrugExposureSourceConcept = TRUE,
  useProcedureOccurrenceSourceConcept = TRUE,
  useMeasurementSourceConcept = TRUE,
  useDeviceExposureSourceConcept = TRUE,
  useObservationSourceConcept = TRUE,
  temporalStartDays = -365:-1,
  temporalEndDays = -365:-1
)
```

## Arguments

- useConditionOccurrenceSourceConcept:

  Logical indicating whether to include condition occurrence source
  concepts.

- useDrugExposureSourceConcept:

  Logical indicating whether to include drug exposure source concepts.

- useProcedureOccurrenceSourceConcept:

  Logical indicating whether to include procedure occurrence source
  concepts.

- useMeasurementSourceConcept:

  Logical indicating whether to include measurement source concepts.

- useDeviceExposureSourceConcept:

  Logical indicating whether to include device exposure source concepts.

- useObservationSourceConcept:

  Logical indicating whether to include observation source concepts.

- temporalStartDays:

  Vector of integers representing the start days of the temporal window
  for covariate extraction.

- temporalEndDays:

  Vector of integers representing the end days of the temporal window
  for covariate extraction.

## Value

Settings for temporal source covariates suitable for feature extraction.

## Details

This function generates settings for temporal source covariates, which
can be used in subsequent feature extraction tasks. It allows specifying
which types of source concepts to include and the temporal window for
extraction.
