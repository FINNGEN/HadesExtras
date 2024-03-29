#
###################################
#### CURRENT FILE: DEV SCRIPT #####
###################################

# Engineering

## Dependencies ----
usethis::use_tibble()

## Add one line by package you want to add as dependency
usethis::use_package( "dplyr" )
usethis::use_package( "stringr")
usethis::use_package( "readr")
usethis::use_package( "lubridate")
#
usethis::use_package( "DatabaseConnector")
usethis::use_package( "SqlRender")
usethis::use_package( "ROhdsiWebApi", type = "Suggests")
usethis::use_package( "here", type = "Suggests")
#
usethis::use_package( "checkmate")
usethis::use_package( "yaml", type = "Suggests")
usethis::use_package( "validate")


usethis::use_package( "devtools", type = "Suggests")
usethis::use_package( "ggplot2", type = "Suggests")




## Add internal datasets ----
## If you have data in your package
#usethis::use_data_raw( name = "my_dataset", open = FALSE )


## Tests ----
## Add one line by test you want to create
usethis::use_testthat()
usethis::use_test("readDatabasesSettings")
#usethis::use_test( "createCDMWebAPIconn" )

# Documentation

## Vignette ----
usethis::use_vignette("connection_tutorial")
#devtools::build_vignettes()

## Code coverage ----
## (You'll need GitHub there)
#usethis::use_github()
#usethis::use_travis()
#usethis::use_appveyor()



