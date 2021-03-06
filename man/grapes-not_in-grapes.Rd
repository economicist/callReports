% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/helpers.R
\name{\%not_in\%}
\alias{\%not_in\%}
\title{Value Anti-Matching}
\usage{
x \%not_in\% table
}
\arguments{
\item{x}{A scalar value of any type}

\item{table}{A vector of possible values to \emph{NOT} match \code{x} against}
}
\value{
\code{TRUE} if \code{x} is not an element of \code{table}, \code{FALSE} if it is.
}
\description{
\verb{\%not_in\%} is a negation of the \code{base} binary operator \code{\%in\%}, and returns
\code{TRUE} if the scalar value \code{x} is \emph{not} an element of the vector \code{table}.
}
\examples{
1 \%not_in\% 2:4 ## [1] TRUE
1 \%not_in\% 1:4 ## [1] FALSE
}
