


# webAPI url for the Atlas demo
baseUrl <- "https://api.ohdsi.org/WebAPI"


# Fracture Cohorts
cohortIds <- c(1783545, 1783544)

cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
  baseUrl = baseUrl,
  cohortIds = cohortIds
)

CohortGenerator::saveCohortDefinitionSet(
  cohortDefinitionSet,
  settingsFileName = "inst/testdata/fracture/Cohorts.csv",
  jsonFolder = "inst/testdata/fracture/cohorts",
  sqlFolder = "inst/testdata/fracture/sql/sql_server",
  cohortFileNameFormat = "%s",
  cohortFileNameValue = c("cohortId"),
  subsetJsonFolder = "inst/testdata/fracture/cohort_subset_definitions/",
  verbose = FALSE
)


# Asthma Cohorts
cohortIds <- c(1783699, 1783700)

cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
  baseUrl = baseUrl,
  cohortIds = cohortIds
)

CohortGenerator::saveCohortDefinitionSet(
  cohortDefinitionSet,
  settingsFileName = "inst/testdata/asthma/Cohorts.csv",
  jsonFolder = "inst/testdata/asthma/cohorts",
  sqlFolder = "inst/testdata/asthma/sql/sql_server",
  cohortFileNameFormat = "%s",
  cohortFileNameValue = c("cohortId"),
  subsetJsonFolder = "inst/testdata/asthma/cohort_subset_definitions/",
  verbose = FALSE
)


# Eunomia Cohorts
cohortIds <- c(1778211, 1778212, 1778213)

cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
  baseUrl = baseUrl,
  cohortIds = cohortIds
)

CohortGenerator::saveCohortDefinitionSet(
  cohortDefinitionSet,
  settingsFileName = "inst/testdata/eunomia/Cohorts.csv",
  jsonFolder = "inst/testdata/eunomia/cohorts",
  sqlFolder = "inst/testdata/eunomia/sql/sql_server",
  cohortFileNameFormat = "%s",
  cohortFileNameValue = c("cohortId"),
  subsetJsonFolder = "inst/testdata/eunomia/cohort_subset_definitions/",
  verbose = FALSE
)








