# Using Matching Subset

``` r
library(HadesExtras)
#devtools::load_all(".")
#options("DEBUG_DATABASECONNECTOR_DBPLYR" = FALSE)
```

## Intro

The CohortTableHandler is an extension of `CDMHandled` (see vignette) to
include the a `cohort` table and the functions to work with this table.

## Getting an Eunomia database for testing

A testing database can be downloaded from Eunomia. See
[Eunomia](https://ohdsi.github.io/Eunomia/reference/getDatabaseFile.html)
for more details.

``` r
# Set EUNOMIA_DATA_FOLDER if not already set
if (Sys.getenv("EUNOMIA_DATA_FOLDER") == "") {
    Sys.setenv(EUNOMIA_DATA_FOLDER = tempdir())
}

# Get the path to the Eunomia database
pathToGiBleedEunomiaSqlite <- Eunomia::getDatabaseFile("GiBleed", overwrite = FALSE)
#> attempting to download GiBleed
#> attempting to extract and load: /tmp/RtmpbP71TN/GiBleed_5.3.zip to: /tmp/RtmpbP71TN/GiBleed_5.3.sqlite
```

## Configuration

Create a `CohortTableHandles` with a test cohort.

``` r
config_yaml <- "
  database:
    databaseId: E1
    databaseName: GiBleed
    databaseDescription: Eunomia database GiBleed
  connection:
    connectionDetailsSettings:
        dbms: sqlite
        server: <pathToGiBleedEunomiaSqlite>
  cdm:
    cdmDatabaseSchema: main
    vocabularyDatabaseSchema: main
  cohortTable:
    cohortDatabaseSchema: main
    cohortTableName: test_cohort_table
"
pathToConfigYaml <- file.path(tempdir(), "config.yml")
writeLines(config_yaml, pathToConfigYaml)
```

``` r
config <- readAndParseYaml(pathToConfigYaml, pathToGiBleedEunomiaSqlite = pathToGiBleedEunomiaSqlite)
```

## Create CohortTableHandler

As in `CDMHandled`, to create a `CohortTableHandles`, for convenience
`createCohortTableHandlerFromList` can be used.

``` r
cohortTableHandler <- createCohortTableHandlerFromList(config)
#> Connecting using SQLite driver
#> Inserting data took 0.00886 secs
#> Creating cohort tables
#> - Created table main.test_cohort_table
#> - Created table main.test_cohort_table
#> - Created table main.test_cohort_table_inclusion
#> - Created table main.test_cohort_table_inclusion_result
#> - Created table main.test_cohort_table_inclusion_stats
#> - Created table main.test_cohort_table_summary_stats
#> - Created table main.test_cohort_table_censor_stats
#> - Created table main.test_cohort_table_checksum
#> Creating cohort tables took 0.08secs
```

In addition to the checks performed by `CDMHandled`,
`cohortTableHandler` includes a check on the creation of the `cohort`
table.

``` r
cohortTableHandler$connectionStatusLog |> 
  reactable_connectionStatus()
```

``` r

# cohort 10:
# 1 M born in 1970
# 1 F born in 1971
#
# cohort 20:
# 10 M born in 1970
# 10 F born in 1970
# 10 F born in 1971
# 10 F born in 1972

cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
  settingsFileName = here::here("inst/testdata/matching/Cohorts.csv"),
  jsonFolder = here::here("inst/testdata/matching/cohorts"),
  sqlFolder = here::here("inst/testdata/matching/sql/sql_server"),
  cohortFileNameFormat = "%s",
  cohortFileNameValue = c("cohortId"),
  #packageName = "HadesExtras",
  verbose = FALSE
)
#> Loading cohortDefinitionSet
```

## Create a matching subset

``` r
# Match to sex and bday, match ratio 10
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "test",
    definitionId = 20,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 10,
        matchRatio = 10,
        matchSex = TRUE,
        matchBirthYear = TRUE,
        matchCohortStartDateWithInDuration = FALSE,
        newCohortStartDate = "keep",
        newCohortEndDate = "keep"
      )
    )
  )
```

``` r

cohortDefinitionSetWithSubsetDef <- cohortDefinitionSet |>
  CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 20)

cohortDefinitionSetWithSubsetDef |>  tibble::as_tibble() |> dplyr::select(-sql, -json)
#> # A tibble: 3 × 6
#>   cohortId cohortName subsetParent isSubset isTemplatedCohort subsetDefinitionId
#>      <dbl> <chr>             <dbl> <lgl>    <lgl>                          <dbl>
#> 1       10 Matching …           10 FALSE    FALSE                             NA
#> 2       20 Matching …           20 FALSE    FALSE                             NA
#> 3    20020 Matching …           20 TRUE     FALSE                             20
```

Build cohorts

``` r
cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSetWithSubsetDef )
#> Skipping cohorts already generated:  
#> Initiating cluster consisting only of main thread
#> 1/3- Generating cohort: Matching cases (id = 10)
#>   |                                                                              |                                                                      |   0%  |                                                                              |===================================                                   |  50%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.00575 secs
#> 2/3- Generating cohort: Matching controls (id = 20)
#>   |                                                                              |                                                                      |   0%  |                                                                              |===================================                                   |  50%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.00552 secs
#> 3/3- Generating cohort: Matching controls - test (id = 20020)
#>   |                                                                              |                                                                      |   0%  |                                                                              |=======                                                               |  10%  |                                                                              |==============                                                        |  20%  |                                                                              |=====================                                                 |  30%  |                                                                              |============================                                          |  40%  |                                                                              |===================================                                   |  50%  |                                                                              |==========================================                            |  60%  |                                                                              |=================================================                     |  70%  |                                                                              |========================================================              |  80%  |                                                                              |===============================================================       |  90%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.00935 secs
#> Generating cohort set took 0.24 secs
#> getCohortDemograpics took 0.0814 secs
```

``` r
cohortsSummary <- cohortTableHandler$getCohortsSummary()
```

``` r
rectable_cohortsSummary(cohortsSummary)
```
