% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/CohortGenerator.R
\name{CohortGenerator_createCohortTables}
\alias{CohortGenerator_createCohortTables}
\title{CohortGenerator_createCohortTables}
\usage{
CohortGenerator_createCohortTables(
  connectionDetails = NULL,
  connection = NULL,
  cohortDatabaseSchema,
  cohortTableNames = CohortGenerator::getCohortTableNames(),
  incremental = FALSE
)
}
\arguments{
\item{connectionDetails}{An object of class \code{connectionDetails} containing database connection details.}

\item{connection}{A database connection object created using \code{DatabaseConnector::connect}.}

\item{cohortDatabaseSchema}{The schema where the cohort tables will be created.}

\item{cohortTableNames}{A list containing the names of the cohort tables to be created.}

\item{incremental}{Logical. If TRUE, only creates tables that don't already exist.}
}
\description{
Wrapper for CohortGenerator::createCohortTables, where if bigquery is used, the tables are created using bigrquery::bq_table_create.
}
