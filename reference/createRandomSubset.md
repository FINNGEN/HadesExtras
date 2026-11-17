# Create a deterministic random cohort subset

The operator samples distinct subjects from the target cohort using a
deterministic ordering derived from `randomSeed` and `subject_id`. All
cohort rows for selected subjects are retained. Repeating the operation
with the same target cohort and seed produces the same subject sample.

## Usage

``` r
createRandomSubset(name = NULL, numberOfSubjects, randomSeed = 20261023)
```

## Arguments

- name:

  Optional name of the operator.

- numberOfSubjects:

  Number of distinct subjects to retain.

- randomSeed:

  Integer seed used to make the sample reproducible.

## Value

A `RandomSubsetOperator` instance.
