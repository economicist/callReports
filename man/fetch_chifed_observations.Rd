% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/db_chifed.R
\name{fetch_chifed_observations}
\alias{fetch_chifed_observations}
\title{Retreive Chicago Fed observations from a database}
\usage{
fetch_chifed_observations(tbl_name, ..., min_date = NULL, max_date = NULL)
}
\arguments{
\item{...}{Quoted variable names, separated by commas.}

\item{db_connector}{A \code{function} created by one of the \verb{db_connector_*()}
functions found in this package. It should be passed without the \verb{()}}
}
\value{
A \code{tibble()} containing bank ID, report date, and values for the
variables requested in \code{...}
}
\description{
Retreive Chicago Fed observations from a database
}
\examples{
chifed_db <- sqlite_connector("./db/chifed.sqlite")
query_chifed_db("RCON2170", "RCON2950", "RCON3210")
}
