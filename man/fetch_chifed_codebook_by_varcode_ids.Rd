% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/db_chifed.R
\name{fetch_chifed_codebook_by_varcode_ids}
\alias{fetch_chifed_codebook_by_varcode_ids}
\title{Fetch the Chicago Fed codebook data for a set of database variable ID codes.}
\usage{
fetch_chifed_codebook_by_varcode_ids(db_connector, varcode_ids)
}
\arguments{
\item{db_connector}{A \code{function} created by one of the \verb{db_connector_*()}
functions found in this package. It should be passed without the \verb{()}}

\item{varcode_ids}{A vector of integer-valued variable code IDs that can
be found in the \code{VARCODES} database table.}
}
\description{
Fetch the Chicago Fed codebook data for a set of database variable ID codes.
}
