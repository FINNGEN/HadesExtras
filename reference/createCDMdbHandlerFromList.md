# createCDMdbHandlerFromList

A function to create a CDMdbHandler object from a list of configuration
settings.

## Usage

``` r
createCDMdbHandlerFromList(config, loadConnectionChecksLevel = "allChecks")
```

## Arguments

- config:

  A list containing configuration settings for the CDMdbHandler.

  - databaseName: The name of the database.

  - connection: A list of connection details settings.

  - cdm: A list of CDM database schema settings.

  - cohortTable: The name of the cohort table.

- loadConnectionChecksLevel:

  The level of checks to perform when loading the connection.

## Value

A CDMdbHandler object.
