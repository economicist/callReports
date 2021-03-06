% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/db_chifed.R
\name{available_chifed_dates}
\alias{available_chifed_dates}
\title{Which periods of data have already been extracted to the Chicago Database?}
\usage{
available_chifed_dates(db_connector)
}
\arguments{
\item{db_connector}{A \code{function} created by one of the \verb{db_connector_*()}
functions found in this package. It should be passed without the \verb{()}}
}
\value{
A character vector containing dates of already-extracted datasets
in the Chicago database, in ISO 'YYYY-MM-DD' format
}
\description{
Which periods of data have already been extracted to the Chicago Database?
}
