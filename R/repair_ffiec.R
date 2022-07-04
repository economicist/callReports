#' Repair a broken FFIEC schedule file
#' 
#' `repair_ffiec_schedule()` takes the path to an unzipped FFIEC schedule file
#' suspected to contain broken observations, and repairs them if found, returning
#' either the path to the new file if changed, or the path to the unchanged
#' file otherwise.
#' 
#' Checks to see if all lines in a tab-separated schedule file have an equal
#' number of tabs. This needs to be the case for the schedule file to be
#' read by `read_tsv()`. The original data has some variables representing
#' additional free-response information provided by the bank reporter. The
#' responses sometimes contain hard returns that are included in the schedule
#' file as unescaped newlines, which breaks up the observation across multiple
#' lines and renders it unparsable.
#'
#' If the lines of the file all contain equal numbers of tabs, then we're
#' finished attempting to repair. Return the path to the submitted schedule
#' file.
#'
#' If the lines of the file do not contain equal numbers of tabs, check to see
#' if the file can be repaired by joining adjacent lines. If it can be, do so,
#' write the repaired schedule to disk, and return the path to the repaired
#' schedule file. Otherwise, return NULL if the file is irreparable.
#' @param sch_unzipped The path to a valid FFIEC schedule file on disk
#' @return A character value containing the path to the repaired file, or the
#' unchanged file if there were no repairs to be made
#' @export
#' @examples
#' repair_ffiec_schedule('FFIEC CDR Call Schedule RIE 06302004.txt')
repair_ffiec_schedule <- function(sch_unzipped) {
  rlog::log_info(glue::glue('Checking integrity of {sch_unzipped}...'))
  var_codes <- read_ffiec_varcodes(sch_unzipped)
  var_descs <- read_ffiec_vardescs(sch_unzipped)
  
  if (length(var_codes) != length(var_descs)) {
    rlog::log_info('Unequal number of values in name and description rows.')
    stop(glue::glue('{sch_unzipped} not a valid schedule file and cannot be repaired.'))
  }
  
  num_vars   <- length(var_codes)
  exp_n_tabs <- num_vars - 1
  obs_lines  <- 
    readr::read_lines(sch_unzipped, skip = 2, 
               skip_empty_rows    = TRUE,
               progress           = FALSE) %>%
    subset( !stringr::str_detect(., '^[[:space:]]*$') )
  df_lines <-
    purrr::map_dfr(obs_lines, function(l) {
      list(n_tabs       = stringr::str_count(l, '\\t'),
           possible_id  = stringr::str_match(l, '^([[:digit:]]+)\t')[2],
           raw_contents = l)
    }) %>%
    dplyr::mutate(possible_id = fill_na_with_previous(.$possible_id))
  
  if (all(df_lines$n_tabs == exp_n_tabs)) {
    rlog::log_info('Lines all have expected number of tabs. Nothing to fix.')
    return(sch_unzipped)
  }
  rlog::log_info('OK. Found irregularities in number of tabs per line. Repairing!')
  
  # Some schedules have problems that need to be directly addressed, usually
  # an unwanted literal tab character in one of the free-response variables,
  # which confuses `read_tsv()` into thinking the line has an invalid number
  # of observations. If this is all that needs fixing, it's faster to just fix
  # it directly and skip the general solution below.
  
  if (stringr::str_detect(sch_unzipped, 'RIE 06302004')) {
    # This schedule has an unwanted tab character in `TEXT4468` for the bank
    # with `IDRSSD` of `490937`. It occurs in the phrase "Other[TAB]ns Exp".
    # Remove that tab using `stringr::str_replace()`
    rlog::log_info("This schedule needs a specific repair. Doing it now.")
    df_lines %<>% dplyr::mutate(
      raw_contents = 
        ifelse(possible_id == "490937", 
               stringr::str_replace(raw_contents, 'Other\tns Exp', 'Other ns Exp'),
               raw_contents))
    new_filename <- paste0(sch_unzipped, '.fixed')
    
    fixed_lines <- c(readr::read_lines(sch_unzipped, n_max = 2),
                     df_lines$raw_contents)
    suppressMessages(readr::write_lines(fixed_lines, new_filename))
    return(new_filename)
  }
  
  # Some schedule files are invalidated only by the presence of unnecessary hard
  # returns (instead of properly escaped newline characters) in the text of some
  # free-response variables. This is true in many of the NARR and RIE files
  #
  # When this is the case, some lines will contain fewer tab-separated values
  # than there are variables in the data set. For example, a dataset containing
  # 57 variables may have a sequence of three lines containing 52, 3, and 2
  # values each. This adds up to the 57 values that are expected. If the data
  # set is valid, then summing up the number of values by row will yield a
  # multiple of the number of variables. Check if that's the case before going
  # row by row.
  
  n_tabs_total <- sum(df_lines$n_tabs)
  if (n_tabs_total %% exp_n_tabs != 0) {
    rlog::log_info('Total tabs in file not a multiple of expected # per line, and')
    rlog::log_info('no specific repair has been written for it.')
    print(df_lines %>% dplyr::filter(n_tabs != exp_n_tabs))
    rlog::log_info(glue::glue(
      'Total tabs: {n_tabs_total}, # Expected Per Line: {exp_n_tabs}'))
    return(NULL)
  }
  
  rlog::log_info('Total # of tabs is multiple of expected # of tabs per line.')
  rlog::log_info('Checking if broken lines can be combined into valid lines.')
  df_lines %<>%
    dplyr::group_by(possible_id) %>%
    dplyr::mutate(sums_to_expected = sum(n_tabs) == exp_n_tabs)
  
  # If the total number of tabs in the schedule file is a multiple of the 
  # number of tabs expected in a valid observation line, then attempt to
  # join consecutive lines.
  
  if (all(df_lines$sums_to_expected)) {
    rlog::log_info('Consecutive broken lines have tab count summing to expected #.')
    rlog::log_info('Proceeding with repair of this schedule file...')
    good_lines <- 
      dplyr::filter(df_lines, n_tabs == exp_n_tabs) %>% .$raw_contents
    fixed_lines <-
      df_lines %>%
      dplyr::filter(n_tabs != exp_n_tabs) %>%
      dplyr::select(possible_id, raw_contents) %>%
      dplyr::summarize(fixed_line = paste0(raw_contents, collapse = '\\n')) %>%
      .$fixed_line
    header_rows <- readr::read_lines(sch_unzipped, n_max = 2, progress = FALSE)
    sch_repaired_lines <- c(header_rows, good_lines, fixed_lines)
    sch_repaired_file  <- paste0(sch_unzipped, '.fixed')
    suppressMessages(readr::write_lines(sch_repaired_lines, sch_repaired_file))
    return(sch_repaired_file)
  }
  
  rlog::log_info("Joining broken lines won't yield rows of equal lengths, and no")
  rlog::log_info('specific repair has been written for this schedule file. You will')
  rlog::log_info('need to investigate this schedule and implement  a specific fix.')
  return(NULL)
}
