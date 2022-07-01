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
#' > str_split1('Hello world')
#' [1] "Hello" "world"
str_split1 <- function(str, pattern) {
  stringr::str_split(str, pattern)[[1]]
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
#' > fill_na_with_previous(c(1, 2, NA, NA, 3))
#' [1] 1 2 2 2 3
fill_na_with_previous <- function(x) {
  purrr::reduce(x, function(a, b) {
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
#' @export
confirm_and_delete <- function(file_path, prompt = glue::glue('Delete {file_path}?')) {
  # If the file isn't there to delete, pretend we've deleted it.
  if (!file.exists(file_path)) return(TRUE)
  
  # If it is, prompt the user for confirmation to delete it.
  permission <- readline(prompt = glue::glue('{prompt} [y/N]: ')) %>% toupper()
  
  # Delete the file if permission to delete is granted.
  if (permission %in% c('Y', 'YES', 'T', 'TRUE')) {
    rlog::log_info(glue::glue('Deleting {file_path}...'))
    unlink(file_path)
    return(TRUE)
  }
  
  # Signal an error if permission to delete is denied.
  stop(glue::glue('Permission to delete {file_path} denied. Exiting...'))
}

#' Get the components of a schedule filename in easily-referenced format.
#'
#' @param sch The name of an FFIEC tab-separated schedule file.
#' @return A named vector containing a value for `report_date`, `sch_code`,
#' `part_num` (default `1`) and `part_of` (default `1`)
#' and number of parts available for the schedule.
#' @export
#' @examples
#' > schedule_name_components('FFIEC CDR Call Schedule RCB 03312012(1 of 2).txt')
#'  report_date     sch_code     part_num      part_of 
#' "2012-03-31"        "RCB"          "1"          "2" 
schedule_name_components <- function(sch) {
  sch_info <- c(report_date = extract_ffiec_datestr(sch),
                sch_code    = extract_schedule_code(sch))
  part_codes <- extract_part_codes(sch)
  return(c(sch_info, part_codes))
}

#' Extract the variable names from an FFIEC schedule file
#'
#' @param sch_unzipped The path to an FFIEC schedule file on disk.
#' @return A character vector containing the variable names found in the top line.
#' @export
#' @examples
#' extract_ffiec_names('FFIEC CDR Call Schedule RCCII 06302002.txt')
extract_ffiec_names <- function(sch_unzipped) { 
  readr::read_lines(sch_unzipped, n_max = 1, progress = FALSE) %>% 
    str_split1('\\t')
}

#' Extract the variable descriptions from an FFIEC schedule file
#'
#' @param sch_unzipped The path to an FFIEC schedule file on disk.
#' @return A character vector containing the variable descriptions found in the 
#' second line of the file.
#' @export
#' @examples
#' extract_ffiec_descs('FFIEC CDR Call Schedule RCCII 06302002.txt')
extract_ffiec_descs <- function(sch_unzipped) { 
  readr::read_lines(sch_unzipped, skip = 1, n_max = 1, progress = FALSE) %>%
    str_split1('\\t')
}

#' Extract the date from an FFIEC schedule filename in `YYYY-MM-DD` format
#'
#' @param filename The name of an FFIEC zip or schedule file, existent or not.
#' Really just any character value with an eight-digit string that can be 
#' interpreted as a date in `MMDDYYYY` format.
#' @return A character value in `YYYY-MM-DD` format containing the date of the
#' report corresponding to that schedule filename.
#' @export
#' @examples
#' extract_ffiec_datestr('FFIEC CDR Call Schedule RCCII 06302002.txt')
extract_ffiec_datestr <- function(filename) {
  as.character(lubridate::mdy(stringr::str_extract(filename, '[[:digit:]]{8}')))
}

#' Extract the alphabetical schedule code from an FFIEC schedule filename
#'
#' @param sch The name of an FFIEC schedule file, existent or not.
#' @return A character value containing the alphabetical schedule code 
#' corresponding to the given schedule filename.
#' @export
#' @examples
#' > extract_schedule_code('FFIEC CDR Call Schedule RCCII 06302002.txt')
#' [1] "RCCII"
extract_schedule_code <- function(sch) {
  stringr::str_match(sch, '([[:alpha:]]+) [[:digit:]]{8}')[1, 2]
}

#' Extract the schedule part number from an FFIEC schedule filename
#'
#' If data for a schedule is issued in one part (true for most), then it will
#' be determined to be part 1 of 1.
#'
#' @param sch The name of an FFIEC schedule file, existent or not.
#' @return A numeric vector with two named values: `part_num` and `part_of`,
#' with both being `1` if no part is indicated in the filename.
#' @export
#' @examples
#' > extract_part_code('FFIEC CDR Call Schedule RCB 03312013(2 of 2).txt')
#' part_num  part_of 
#'        1        2 
extract_part_codes <- function(sch) {
  rx <- '([[:digit:]]{8})(\\(([[:digit:]]) of ([[:digit:]])\\))*'
  part_codes <- stringr::str_match(sch, rx)
  if (is.na(part_codes[1, 3])) return(c(part_num = 1, part_of = 1))
  return(c(part_num = as.numeric(part_codes[1, 4]), 
           part_of  = as.numeric(part_codes[1, 5])))
}

#' Extract the values from one line of an FFIEC schedule file
#' 
#' @param sch_line A line from an FFIEC tab-separated schedule file
#' @return A character vector of the values in that line
#' @export
#' @examples
#' extract_values('12311  2031  298310')
#' [1] "12311" "2031" "298310"
extract_values <- function(sch_line) {
  sch_line %>% str_split1('\\t')
}

#' Convert a quarter ID back into a date
#'
#' The Chicago Fed and FFIEC source data altogether run quarterly from the
#' first quarter of 1976 to the present (roughly, the last quarter that ended
#' roughly at least 6 weeks ago). This function converts either a `Date` or a
#' string containing a date in ISO `YYYY-MM-DD` format into a small integer ID
#' value, where `1976-Q1` is represented by `1` and incrementing from there.
#' 
#' Pivoting the source data into long form before writing it to the database
#' has the benefit of giving the database tables a simple and comprehensible
#' structure, but requires each date to be written thousands or millions of
#' times due to the sheer number of bank-variable pairs each period.
#' 
#' Creating an integer ID to index the dates allows the date column in each
#' table to be stored as an integer, which requires significantly less space (2 
#' bytes max) than even the most conservative string format that keeps the date
#' readable (8 bytes). This significantly reduces the storage requirement of the
#' extracted data sets, and potentially improves query performance when the
#' database is kept on slower storage devices such as spinning-platter hard
#' drives.
#' 
#' The extraction functions found in this library use this function's inverse,
#' `date_str_to_qtr_id()`, to convert the quarter-end reporting dates found in
#' the source data into integer-valued quarter IDs.
#' 
#' @param qtr_int An `integer` representing the index of a quarter since the
#' beginning of year 1976
#' @return The `Date` of the last day of the requested quarter.
#' @export
qtr_id_to_date_str <- function(qtr_int) {
  yyyy <- 1976 + (qtr_int %/% 4)
  qq   <- ifelse(qtr_int %% 4 == 0, 4, qtr_int %% 4)
  next_qtr_start <-
    glue::glue('{yyyy}-{qq}') %>%
    lubridate::yq() %>% 
    lubridate::ceiling_date('quarter')
  this_qtr_end <- next_qtr_start - lubridate::days(1)
  return(this_qtr_end)
}

#' Convert a reporting date into an integer-valued quarter ID
#'
#' The Chicago Fed and FFIEC source data altogether run quarterly from the
#' first quarter of 1976 to the present (roughly, the last quarter that ended
#' roughly at least 6 weeks ago). This function converts either a `Date` or a
#' string containing a date in ISO `YYYY-MM-DD` format into a small integer ID
#' value, where `1976-Q1` is represented by `1` and incrementing from there.
#' 
#' Pivoting the source data into long form before writing it to the database
#' has the benefit of giving the database tables a simple and comprehensible
#' structure, but requires each date to be written thousands or millions of
#' times due to the sheer number of bank-variable pairs each period.
#' 
#' Creating an integer ID to index the dates allows the date column in each
#' table to be stored as an integer, which requires significantly less space (2 
#' bytes max) than even the most conservative string format that keeps the date
#' readable (8 bytes). This significantly reduces the storage requirement of the
#' extracted data sets, and potentially improves query performance when the
#' database is kept on slower storage devices such as spinning-platter hard
#' drives.
#' 
#' Query functions found in this library use this function's inverse,
#' `qtr_id_to_date_str()`, which converts quarter IDs back into readable dates
#' for output to the end user.
#'
#' @param date_str A `Date` or character value in ISO `YYYY-MM-DD` format
#' @return The index of the quarter since the beginning of the year 1976, the
#' initial year of data available from the Chicago Federal Reserve.
#' @export
date_str_to_qtr_id <- function(date_str) {
  yyyy <- lubridate::year(lubridate::ymd(date_str))
  qq   <- lubridate::quarter(lubridate::ymd(date_str))
  return(4 * (yyyy - 1976) + qq)
}

#' Rebuild an FFIEC schedule filename from its relevant parts
#'
#' From the reporting date, schedule code, part number and part code given as
#' parameters, construct a filename in the pattern of those found in the zipped
#' FFIEC schedule files.
#'
#' The `SUMMARY` table in the FFIEC database contains five columns that indicate
#' the dates, schedule codes, and part numbers of FFIEC schedule files that have
#' already been extracted to the database. This function takes parameters 
#' describing a hypothetical schedule file, in the format they would be seen in
#' the aforementioned `SUMMARY` table, and outputs a filename in the pattern
#' `FFIEC CDR Call Schedule {sch_code} {rep_date}{part_str}.txt`, where
#' `part_str` is simply the `NULL` string when `part_of` is equal to `1`, as only
#' those schedule files that are in fact divided into parts have their part
#' numbers indicated in their filename.
#'
#' @param rep_date A date string in ISO `YYYY-MM-DD` format
#' @param sch_code An FFIEC schedule code
#' @param part_num A part number
#' @param part_of The number of parts the schedule described by these parameters
#' is divided into.
#' @return A character valued filename constructed from the given parameters.
#' @export
build_schedule_filename <- function(rep_date, sch_code, part_num, part_of) {
  part_str <- 
    ifelse(as.numeric(part_of) == 1, '', glue::glue('({part_num} of {part_of})'))
  sch_filename <- 
    glue::glue('FFIEC CDR Call Schedule {sch_code} {rep_date}{part_str}.txt')
  return(sch_filename)
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
#' > generate_log_name('extraction_attempt')
#' [1] "extraction_attempt_20220628_141233.log.txt"
generate_log_name <- function(prefix) {
  prefix %<>% stringr::str_replace_all('\\s+', '_')
  timestamp <- 
    Sys.time() %>% stringr::str_replace_all('[-:]', '') %>% stringr::str_replace('\\s', '_')
  return(glue::glue('{prefix}_{timestamp}.log.txt'))
}
