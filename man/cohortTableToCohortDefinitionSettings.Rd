% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/cohortTable.R
\name{cohortTableToCohortDefinitionSettings}
\alias{cohortTableToCohortDefinitionSettings}
\title{Convert Cohort Table to Cohort Definition Settings}
\usage{
cohortTableToCohortDefinitionSettings(
  cohortDatabaseSchema,
  cohortTable = "cohort",
  cohortDefinitionTable,
  cohortDefinitionIds,
  newCohortDefinitionIds = cohortDefinitionIds
)
}
\arguments{
\item{cohortDatabaseSchema}{The database schema where the cohort table is located.}

\item{cohortTable}{The name of the cohort table. Defaults to "cohort".}

\item{cohortDefinitionTable}{The name of the cohort definition table where settings will be stored.}

\item{cohortDefinitionIds}{Numeric vector specifying cohort IDs to generate settings for.}

\item{newCohortDefinitionIds}{Numeric vector specifying new cohort IDs to assign to the settings.}
}
\value{
A tibble containing cohort definition settings.
}
\description{
Converts cohort table entries into cohort definition settings for specified cohort IDs within a given database schema.
This function is useful for generating cohort definition settings from existing cohort data.
}
