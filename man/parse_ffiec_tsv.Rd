% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/extract_ffiec.R
\name{parse_ffiec_tsv}
\alias{parse_ffiec_tsv}
\title{Parse an FFIEC TSV file into codebook, observation, and problem data frames.}
\usage{
parse_ffiec_tsv(tsv)
}
\arguments{
\item{tsv}{The path to an unzipped FFIEC TSV schedule file}
}
\value{
A named \code{list} with three \code{tibble} elements: \code{codebook}, \code{observations},
and \code{problems}
}
\description{
\code{parse_ffiec_tsv()} accepts a text file containing tab-separated-value
bulk data provided by the FFIEC, and writes it to a given database.
}
\details{
ZIP files containing valid schedule files for 2001-present are offered at the
\href{https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx}{FFIEC Bulk Data Download Service}
}
\examples{
parse_ffiec_tsv("FFIEC CDR Call Schedule RCCII 06302002.txt")
}
