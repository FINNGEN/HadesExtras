# Resolve Cohort Short Name Conflicts

Given a set of new and existing short names, resolve any duplicates.
Appends an index to any duplicate names to ensure uniqueness.

## Usage

``` r
.solveShortNameConflicts(newShortNames, existingShortNames)
```

## Arguments

- newShortNames:

  Character vector. The newly generated short names to be made unique.

- existingShortNames:

  Character vector. The set of existing short names to avoid conflicts
  with.

## Value

A character vector of unique short names.
