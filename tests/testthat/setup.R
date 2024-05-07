# settings
configurationName  <- getOption("configurationName", default = "E1")
#configurationName  <- getOption("configurationName", default = "BQ1")
#configurationName  <- getOption("configurationName", default = "BQ500k")
#
configurations <- yaml::read_yaml(testthat::test_path("config", "test_config.yml"))

testSelectedConfiguration <- configurations[[configurationName]]

message("************* Testing on ", configurationName, " *************")


