# Using CohortTableHandler

``` r
library(HadesExtras)
# options("DEBUG_DATABASECONNECTOR_DBPLYR" = FALSE)
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
#> attempting to extract and load: /tmp/RtmpPaOzR5/GiBleed_5.3.zip to: /tmp/RtmpPaOzR5/GiBleed_5.3.sqlite
```

## Configuration

In addition to the configuration for `CDMHandled`, we also need the
configuration indicating the location of the cohort table.

We need the following configuration. We write this in yaml format for
clarity.

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
#> Inserting data took 0.0085 secs
#> Creating cohort tables
#> - Created table main.test_cohort_table
#> - Created table main.test_cohort_table
#> - Created table main.test_cohort_table_inclusion
#> - Created table main.test_cohort_table_inclusion_result
#> - Created table main.test_cohort_table_inclusion_stats
#> - Created table main.test_cohort_table_summary_stats
#> - Created table main.test_cohort_table_censor_stats
#> Creating cohort tables took 0.04secs
```

In addition to the checks performed by `CDMHandled`,
`cohortTableHandler` includes a check on the creation of the `cohort`
table.

``` r
cohortTableHandler$connectionStatusLog |>
    reactable_connectionStatus()
```

## Add cohors to `cohort` table

To add cohort to the `cohortTableHandler` object we need a
`cohortDefinitionSet` table (this table is defined in
[CohortGenerator](https://ohdsi.github.io/CohortGenerator/articles/GeneratingCohorts.html#downloading-cohorts-from-atlas)).

`cohortDefinitionSet` can be created from
[Atlas](https://ohdsi.github.io/CohortGenerator/articles/GeneratingCohorts.html#downloading-cohorts-from-atlas)
cohort definitions, or from [study
packages](https://ohdsi.github.io/CohortGenerator/reference/getCohortDefinitionSet.html).

Additionally, `HadesExtras` includes a way to produce
`cohortDefinitionSet` based on a list of local person ids (OMOP table
person field person_source_value).

### Add cohorts from Atlas

Get `cohortDefinitionSet` form 3 cohort definitions in Atlas demo.

``` r
# webAPI url for the Atlas demo
baseUrl <- "https://api.ohdsi.org/WebAPI"
# A list of cohort IDs for use in this vignette
cohortIds <- c(1778211, 1778212, 1778213)
# Get the SQL/JSON for the cohorts
cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
    baseUrl = baseUrl,
    cohortIds = cohortIds
)
```

``` r
cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
)
#> Loading cohortDefinitionSet
```

Build cohorts

``` r
cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
#> Initiating cluster consisting only of main thread
#> 1/4- Generating cohort: celecoxib (id = 1)
#>   |                                                                              |                                                                      |   0%  |                                                                              |===                                                                   |   4%  |                                                                              |======                                                                |   8%  |                                                                              |========                                                              |  12%  |                                                                              |===========                                                           |  16%  |                                                                              |==============                                                        |  20%  |                                                                              |=================                                                     |  24%  |                                                                              |====================                                                  |  28%  |                                                                              |======================                                                |  32%  |                                                                              |=========================                                             |  36%  |                                                                              |============================                                          |  40%  |                                                                              |===============================                                       |  44%  |                                                                              |==================================                                    |  48%  |                                                                              |====================================                                  |  52%  |                                                                              |=======================================                               |  56%  |                                                                              |==========================================                            |  60%  |                                                                              |=============================================                         |  64%  |                                                                              |================================================                      |  68%  |                                                                              |==================================================                    |  72%  |                                                                              |=====================================================                 |  76%  |                                                                              |========================================================              |  80%  |                                                                              |===========================================================           |  84%  |                                                                              |==============================================================        |  88%  |                                                                              |================================================================      |  92%  |                                                                              |===================================================================   |  96%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.0381 secs
#> 2/4- Generating cohort: celecoxibAge40 (id = 2)
#>   |                                                                              |                                                                      |   0%  |                                                                              |==                                                                    |   3%  |                                                                              |=====                                                                 |   7%  |                                                                              |=======                                                               |  10%  |                                                                              |=========                                                             |  13%  |                                                                              |============                                                          |  17%  |                                                                              |==============                                                        |  20%  |                                                                              |================                                                      |  23%  |                                                                              |===================                                                   |  27%  |                                                                              |=====================                                                 |  30%  |                                                                              |=======================                                               |  33%  |                                                                              |==========================                                            |  37%  |                                                                              |============================                                          |  40%  |                                                                              |==============================                                        |  43%  |                                                                              |=================================                                     |  47%  |                                                                              |===================================                                   |  50%  |                                                                              |=====================================                                 |  53%  |                                                                              |========================================                              |  57%  |                                                                              |==========================================                            |  60%  |                                                                              |============================================                          |  63%  |                                                                              |===============================================                       |  67%  |                                                                              |=================================================                     |  70%  |                                                                              |===================================================                   |  73%  |                                                                              |======================================================                |  77%  |                                                                              |========================================================              |  80%  |                                                                              |==========================================================            |  83%  |                                                                              |=============================================================         |  87%  |                                                                              |===============================================================       |  90%  |                                                                              |=================================================================     |  93%  |                                                                              |====================================================================  |  97%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.0334 secs
#> 3/4- Generating cohort: celecoxibAge40Male (id = 3)
#>   |                                                                              |                                                                      |   0%  |                                                                              |==                                                                    |   3%  |                                                                              |====                                                                  |   6%  |                                                                              |======                                                                |   9%  |                                                                              |========                                                              |  12%  |                                                                              |==========                                                            |  15%  |                                                                              |============                                                          |  18%  |                                                                              |==============                                                        |  21%  |                                                                              |================                                                      |  24%  |                                                                              |===================                                                   |  26%  |                                                                              |=====================                                                 |  29%  |                                                                              |=======================                                               |  32%  |                                                                              |=========================                                             |  35%  |                                                                              |===========================                                           |  38%  |                                                                              |=============================                                         |  41%  |                                                                              |===============================                                       |  44%  |                                                                              |=================================                                     |  47%  |                                                                              |===================================                                   |  50%  |                                                                              |=====================================                                 |  53%  |                                                                              |=======================================                               |  56%  |                                                                              |=========================================                             |  59%  |                                                                              |===========================================                           |  62%  |                                                                              |=============================================                         |  65%  |                                                                              |===============================================                       |  68%  |                                                                              |=================================================                     |  71%  |                                                                              |===================================================                   |  74%  |                                                                              |======================================================                |  76%  |                                                                              |========================================================              |  79%  |                                                                              |==========================================================            |  82%  |                                                                              |============================================================          |  85%  |                                                                              |==============================================================        |  88%  |                                                                              |================================================================      |  91%  |                                                                              |==================================================================    |  94%  |                                                                              |====================================================================  |  97%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.0355 secs
#> 4/4- Generating cohort: celecoxibCensored (id = 4)
#>   |                                                                              |                                                                      |   0%  |                                                                              |==                                                                    |   4%  |                                                                              |=====                                                                 |   7%  |                                                                              |========                                                              |  11%  |                                                                              |==========                                                            |  14%  |                                                                              |============                                                          |  18%  |                                                                              |===============                                                       |  21%  |                                                                              |==================                                                    |  25%  |                                                                              |====================                                                  |  29%  |                                                                              |======================                                                |  32%  |                                                                              |=========================                                             |  36%  |                                                                              |============================                                          |  39%  |                                                                              |==============================                                        |  43%  |                                                                              |================================                                      |  46%  |                                                                              |===================================                                   |  50%  |                                                                              |======================================                                |  54%  |                                                                              |========================================                              |  57%  |                                                                              |==========================================                            |  61%  |                                                                              |=============================================                         |  64%  |                                                                              |================================================                      |  68%  |                                                                              |==================================================                    |  71%  |                                                                              |====================================================                  |  75%  |                                                                              |=======================================================               |  79%  |                                                                              |==========================================================            |  82%  |                                                                              |============================================================          |  86%  |                                                                              |==============================================================        |  89%  |                                                                              |=================================================================     |  93%  |                                                                              |====================================================================  |  96%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.0362 secs
#> Generating cohort set took 0.33 secs
#> getCohortDemograpics took 0.12 secs
```

Check cohort counts

``` r
cohortTableHandler$getCohortCounts()
#> # A tibble: 4 × 4
#>   cohortName         cohortId cohortEntries cohortSubjects
#>   <chr>                 <dbl>         <dbl>          <dbl>
#> 1 celecoxib                 1          1800           1800
#> 2 celecoxibAge40            2           569            569
#> 3 celecoxibAge40Male        3           266            266
#> 4 celecoxibCensored         4          1750           1750
```

``` r
cohortsSummary <- cohortTableHandler$getCohortsSummary()
```

``` r
rectable_cohortsSummary(cohortsSummary)
```

Check Cohorts Overlap

``` r
cohortTableHandler$getCohortsOverlap()
#> # A tibble: 6 × 2
#>   cohortIdCombinations numberOfSubjects
#>   <chr>                           <dbl>
#> 1 -1-                                35
#> 2 -1-2-                               7
#> 3 -1-2-3-                             8
#> 4 -1-2-3-4-                         258
#> 5 -1-2-4-                           296
#> 6 -1-4-                            1196
```

### Add cohorts from cohortData file

Create tibble of `cohortData` format. See the documentation for the
correct format.

``` r
cohortData <- tibble::tribble(
    ~cohort_name, ~person_source_value, ~cohort_start_date, ~cohort_end_date,
    "Cohort A", "000728a7-80de-420a-9286-2c20e81cb7b8", as.Date("2020-01-01"), as.Date("2020-01-03"),
    "Cohort A", "000cb58f-523d-49a2-a05e-de1e93f35c01", as.Date("2020-01-01"), as.Date("2020-01-03"),
    "Cohort A", "001f4a87-70d0-435c-a4b9-1425f6928d33", as.Date("2020-01-01"), as.Date("2020-01-03"),
    "Cohort A", "002805e7-7624-4cb7-b68d-e8ac92f61ff9", as.Date("2020-01-01"), as.Date("2020-01-03"),
    "Cohort A", "0030eb48-316c-4250-907f-a272909ff8b9", as.Date("2020-01-01"), as.Date("2020-01-03"),
    "Cohort B", "00093765-abef-4a56-9280-8f92422afae7", as.Date("2020-01-01"), as.Date("2020-01-04"),
    "Cohort B", "00196a95-1567-41f8-b608-18e6295b4c1e", as.Date("2020-01-01"), as.Date("2020-01-04"),
    "Cohort B", "00211cd2-f171-45d9-a7c6-ea8ac6d28d09", as.Date("2020-01-01"), as.Date("2020-01-04"),
    "Cohort B", "0029df47-1263-4576-8ca3-4615adb7dd7a", as.Date("2020-01-01"), as.Date("2020-01-04"),
    "Cohort B", "00444703-f2c9-45c9-a247-f6317a43a929", as.Date("2020-01-01"), as.Date("2020-01-04")
)
```

We can check if the format is correct using `checkCohortData`. See the
documentation for the checks

If correct, It will return TRUE.

``` r
checkCohortData(cohortData)
#> [1] TRUE
```

If any error, It will return an array of strings with the errors
detected.

``` r
cohortDataWithErrors <- cohortData
cohortDataWithErrors[1, 1] <- as.character(NA)
cohortDataWithErrors[1, 3] <- as.Date("2030-01-01")
checkCohortData(cohortDataWithErrors)
#> [1] "1 rows are missing cohort_name"                          
#> [2] "1 rows have cohort_start_date older than cohort_end_date"
```

To copy the `cohortData` into the `cohort`it has to be converted to a
`cohortDefinitionSet`.

``` r
cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohortData
)
```

Copy `cohortDefinitionSet` to `cohort` table

``` r
cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)
#> Warning in cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet):
#> Following cohort ids already exists on the cohort table and will be updated: 1,
#> 2
#> Inserting data took 0.00757 secs
#> Initiating cluster consisting only of main thread
#> 1/4- Generating cohort: Cohort A (id = 1)
#>   |                                                                              |                                                                      |   0%  |                                                                              |===================================                                   |  50%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.0106 secs
#> 2/4- Generating cohort: Cohort B (id = 2)
#>   |                                                                              |                                                                      |   0%  |                                                                              |===================================                                   |  50%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.00818 secs
#> Skipping cohortId = '3' because it is unchanged from earlier run
#> Skipping cohortId = '4' because it is unchanged from earlier run
#> Generating cohort set took 0.08 secs
#> Counting cohorts took 0.0158 secs
#> getCohortDemograpics took 0.0908 secs
```

``` r
cohortsSummary <- cohortTableHandler$getCohortsSummary()
```

``` r
rectable_cohortsSummary(cohortsSummary)
```
