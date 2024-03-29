% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/CohortTableHandler.R
\name{createCohortTableHandlerFromList}
\alias{createCohortTableHandlerFromList}
\title{createCohortTableHandlerFromList}
\usage{
createCohortTableHandlerFromList(
  cohortTableHandlerConfig,
  loadConnectionChecksLevel = "allChecks"
)
}
\arguments{
\item{cohortTableHandlerConfig}{A list containing cohortTableHandlerConfiguration settings for the CohortTableHandler.
\itemize{
\item databaseName: The name of the database.
\item connection: A list of connection details settings.
\item cdm: A list of CDM database schema settings.
\item cohortTable: A list of cohort table settings.
}}

\item{loadConnectionChecksLevel}{(Optional) Level of checks to perform when loading the connection (default is "allChecks").}
}
\value{
A CohortTableHandler object.
}
\description{
A function to create a CohortTableHandler object from a list of cohortTableHandlerConfiguration settings.
}
