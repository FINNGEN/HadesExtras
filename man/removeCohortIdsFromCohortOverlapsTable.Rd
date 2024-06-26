% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/CohortOverlaps.R
\name{removeCohortIdsFromCohortOverlapsTable}
\alias{removeCohortIdsFromCohortOverlapsTable}
\title{Remove specified cohort IDs from cohort overlaps table}
\usage{
removeCohortIdsFromCohortOverlapsTable(cohortOverlaps, cohortIds)
}
\arguments{
\item{cohortOverlaps}{A data frame containing cohort overlaps.}

\item{cohortIds}{A numeric vector of cohort IDs to be removed.}
}
\value{
A data frame with cohort overlaps after removing specified IDs.
}
\description{
This function removes specified cohort IDs from a cohort overlaps table.
}
\examples{
cohortOverlaps <- data.frame(cohortIdCombinations = c("-1-2-", "-2-3-", "-3-4-"),
                            numberOfSubjects = c(10, 15, 20))
cohortIds <- c(2, 3)
removeCohortIdsFromCohortOverlapsTable(cohortOverlaps, cohortIds)
}
