#' Separate a single string into a single character vector
#' 
#' `str_split1()` allows the user to avoid problems associated with using
#' `stringr::str_split()`, which always returns a list, on a single character
#' value. Use the same was as you would `stringr::str_split()` but skip the need
#' to extract the first element from the result.
#'
#' @param str A single character value
#' @param pattern A regular expression on which to split the string
#' @return A character vector containing the split values
#' @importFrom stringr str_split
#' @examples
#' > str_split1('Hello world')
#' [1] "Hello" "world"
str_split1 <- function(str, pattern) {
  str_split(str, pattern)[[1]]
}

#' Replace `NA` values with last-seen non-`NA` value
#'
#' Given a vector `x` containing NA values, accumulate a new output vector by 
#' progressively replacing each NA value with the value of the last non-NA
#' value of an earlier index
#' @param x A vector suspected to contain `NA` values
#' @return A vector containing all non-`NA` values from `x`, but with all `NA`
#' values replaced with the "last-seen" non-`NA` value up to that point in the
#' vector
#' @importFrom purrr reduce
#' @examples
#' > fill_na_with_previous(c(1, 2, NA, NA, 3))
#' [1] 1 2 2 2 3
fill_na_with_previous <- function(x) {
  reduce(x, function(a, b) {
    # `a` is the vector of values we've already validated or filled with NAs
    #     Thus `a[length(a)`, the final value of that vector, is the last non-NA
    # `b` is the next value we're checking in the vector
    last_checked_element <- a[length(a)]
    next_element         <- ifelse(is.na(b), last_checked_element, b)  
    return(c(a, next_element))
  })
}

#' Confirm deletion of an existing file
#'
#' `confirm_and_delete()` confirms whether a user wishes to delete the file
#' given as its argument, does so if the answer is affirmative, and otherwise 
#' ends the execution of the currently-running script with an error.
#'
#' @param prompt A yes/no question to ask the user
#' @return `TRUE` if the user responds with any of "Y", "YES", "T", or "TRUE"
#' (case-insensitive).
#' @importFrom glue glue
confirm_and_delete <- function(file_path, prompt = glue('Delete {file_path}?')) {
  # If the file isn't there to delete, pretend we've deleted it.
  if (!file.exists(file_path)) return(TRUE)
  
  # If it is, prompt the user for confirmation to delete it.
  permission <- readline(prompt = glue('{prompt} [y/N]: ')) %>% toupper()
  
  # Delete the file if permission to delete is granted.
  if (permission %in% c('Y', 'YES', 'T', 'TRUE')) {
    log_info(glue('Deleting {file_path}...'))
    unlink(file_path)
    return(TRUE)
  }
  
  # Signal an error if permission to delete is denied.
  stop(glue('Permission to delete {file_path} denied. Exiting...'))
}