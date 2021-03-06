% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/helpers.R
\name{confirm_and_delete}
\alias{confirm_and_delete}
\title{Confirm deletion of an existing file}
\usage{
confirm_and_delete(file_path, prompt = glue::glue("Delete {file_path}?"))
}
\arguments{
\item{prompt}{A yes/no question to ask the user}
}
\value{
\code{TRUE} if the user responds with any of "Y", "YES", "T", or "TRUE"
(case-insensitive).
}
\description{
\code{confirm_and_delete()} confirms whether a user wishes to delete the file
given as its argument, does so if the answer is affirmative, and otherwise
ends the execution of the currently-running script with an error.
}
