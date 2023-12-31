% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/reactable_connectionStatus.R
\name{reactable_connectionStatus}
\alias{reactable_connectionStatus}
\title{reactable_connectionStatus}
\usage{
reactable_connectionStatus(
  connectionStatus,
  emojis = list(error = "❌", warning = "⚠️", success = "✅", info = "ℹ️")
)
}
\arguments{
\item{connectionStatus}{A data frame containing connection status information.}

\item{emojis}{list of emojis}
}
\value{
A reactable displaying the connection status information.
}
\description{
A function to display connection status in a reactable.
}
