#' Confirm deletion of an existing file
#'
#' `confirm_and_delete()` confirms whether a user wishes to delete the file
#' given as its argument, does so if the answer is affirmative, and otherwise
#' ends the execution of the currently-running script with an error.
#'
#' @param prompt A yes/no question to ask the user
#' @return `TRUE` if the user responds with any of "Y", "YES", "T", or "TRUE"
#' (case-insensitive).
#' @export
confirm_and_delete <-
  function(file_path, prompt = glue::glue("Delete {file_path}?")) {
    # If the file isn't there to delete, pretend we've deleted it.
    if (!file.exists(file_path)) {
      return(TRUE)
    }

    # If it is, prompt the user for confirmation to delete it.
    permission <- toupper(readline(prompt = glue::glue("{prompt} [y/N]: ")))

    # Stop and signal an error if permission is not granted.
    if (permission %not_in% c("Y", "YES", "T", "TRUE")) {
      stop(glue::glue("Permission to delete {file_path} denied. Exiting..."))
    }

    # Otherwise, delete the file
    rlog::log_info(glue::glue("Deleting {file_path}..."))
    unlink(file_path)
    return(TRUE)
  }

#' Create a directory if it doesn't exist
#'
#' `create_dir_if_not_exists()` checks to see if the specified directory exists
#' already. If it does, it returns `NULL`, but creates it otherwise.
#'
#' @param newdir_path A character-valued path to a folder to create
#' @export
create_dir_if_not_exists <- function(newdir_path) {
  if (dir.exists(newdir_path)) {
    return()
  }
  dir.create(newdir_path)
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
#' @export
#' @examples
#' fill_na_with_previous(c(1, 2, NA, NA, 3)) ## [1] 1 2 2 2 3
fill_na_with_previous <- function(x) {
  purrr::reduce(x, function(a, b) {
    # `a` is the vector of values we've already validated or filled with NAs
    #     Thus `a[length(a)`, the final value of that vector, is the last non-NA
    # `b` is the next value we're checking in the vector
    last_checked_element <- a[length(a)]
    next_element <- ifelse(is.na(b), last_checked_element, b)
    return(c(a, next_element))
  })
}

#' Build a log filename with a prefix and the time it was initiated.
#'
#' @param prefix A character value to begin the name of the file. May contain
#' slashes if you want the generated log file to go in a different folder than
#' specified by `getwd()`
#' @return A character value in the form `{prefix}_{datestr}.log.txt`
#' @importFrom magrittr %>% %<>%
#' @export
#' @examples
#' generate_log_name("extraction_attempt") ## [1] "extraction_attempt_20220628_141233.log.txt"
generate_log_name <- function(prefix) {
  logging_dir <- get_logging_dir()
  prefix %<>% stringr::str_replace_all("\\s+", "_")
  timestamp <-
    Sys.time() %>%
    stringr::str_replace_all("[-:]", "") %>%
    stringr::str_replace("\\s", "_")
  return(glue::glue("{logging_dir}/{prefix}_{timestamp}.log.txt"))
}

#' Extract the values from one line of an FFIEC schedule file
#'
#' @param sch_line A line from an FFIEC tab-separated schedule file
#' @return A character vector of the values in that line
#' @export
#' @examples
#' parse_tsv_line("12311  2031  298310") ## [1] "12311" "2031" "298310"
parse_tsv_line <- function(tsv_line) {
  str_split1(tsv_line, "\\t")
}

#' Get the last day of a calendar quarter
#'
#' @param dt A `Date`, or a character value in ISO `YYYY-MM-DD` format
#' @return The `Date` of the final day of the quarter `dt` is in.
#' @export
qtr_end <- function(ymd_chr = Sys.Date()) {
  lubridate::ymd(ymd_chr) %>%
    lubridate::ceiling_date('quarter') -
    lubridate::days(1)
}

#' Repair any invalid column names
#'
#' `vroom::vroom()` display a message about renaming variables with invalid
#' names if no rule is provided for repairing them. This function provides such
#' a rule, and can be used as the `name_repair` parameter of those functions.
#'
#' @param nm A character-valued variable name
#' @param idx The index of that variable in the data set
#' @return A valid and unique variable name
#' @export
repair_colnames <- function(nms, prefix = "UNNAMED") {
  purrr::map2_chr(nms, 1:length(nms), function(nm, idx) {
    ifelse(
      is.null(nm) | stringr::str_length(stringr::str_trim(nm)) == 0,
      glue::glue("{prefix}_{idx}"),
      stringr::str_trim(nm) %>%
        stringr::str_to_upper() %>%
        stringr::str_replace_all("[\\.[:space:]]+", "_")
    )
  })
}

#' Is a character value `NULL`, of length zero, or just whitespace?
#'
#' For the purposes of identifying text fields with no input in the Chicago
#' data, this function checks to see if a value is `NULL`, and more importantly
#' if it is simply a string of length zero or whitespace only.
#'
#' @param val A scalar character value
#' @return `TRUE` if `val` is either `NULL`, a string of length zero, or a string
#' containing only whitespace
#' @export
str_is_blank <- function(val) {
  if (is.null(val)) {
    return(TRUE)
  }
  val_length <- tryCatch(
    {
      # There's an observation of variable `TEXTA546` in `CALL9703.XPT` that has
      # a string being interpreted as invalid Unicode. It's not a character with
      # any semantic value, so just detect and remove it before attempting to
      # determine whether an observation is blank.
      stringr::str_remove_all(val, "\\\\xb0$") %>%
        stringr::str_trim() %>%
        stringr::str_length()
    },
    error = function(e) {
      print(as.vector(val) %>% subset(. != ""))
      stop(e)
    }
  )

  return(val_length == 0)
}

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
#' @export
#' @examples
#' str_split1("Hello world") ## [1] "Hello" "world"
str_split1 <- function(str, pattern) {
  stringr::str_split(str, pattern)[[1]]
}

#' Value Anti-Matching
#'
#' `%not_in%` is a negation of the `base` binary operator `%in%`, and returns
#' `TRUE` if the scalar value `x` is *not* an element of the vector `table`.
#'
#' @param x A scalar value of any type
#' @param table A vector of possible values to *NOT* match `x` against
#' @return `TRUE` if `x` is not an element of `table`, `FALSE` if it is.
#' @export
#' @examples
#' 1 %not_in% 2:4 ## [1] TRUE
#' 1 %not_in% 1:4 ## [1] FALSE
`%not_in%` <- function(x, table) {
  !(x %in% table)
}
