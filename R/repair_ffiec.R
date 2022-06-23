#' Repair a broken FFIEC schedule file
#' 
#' `fix_broken_ffiec_obs()` takes the path to an unzipped FFIEC schedule file
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
#' @importFrom magrittr %>% %<>%
#' @importFrom dplyr filter group_by mutate select summarize
#' @importFrom glue glue
#' @importFrom purrr map_dfr
#' @importFrom readr read_lines write_lines
#' @importFrom rlog log_info log_fatal log_warn
#' @importFrom stringr str_count str_detect str_match str_replace
#' @export
#' @examples
#' fix_broken_ffiec_obs('FFIEC CDR Call Schedule RIE 06302004.txt')
fix_broken_ffiec_obs <- function(sch_unzipped) {
  log_info(glue('Checking integrity of {sch_unzipped}...'))
  var_names <- callReports::extract_ffiec_names(sch_unzipped)
  var_descs <- callReports::extract_ffiec_descs(sch_unzipped)
  
  if (length(var_names) != length(var_descs)) {
    log_fatal('Unequal number of values in name and description rows.')
    stop(glue('{sch_unzipped} not a valid schedule file and cannot be repaired.'))
  }
  
  num_vars   <- length(var_names)
  exp_n_tabs <- num_vars - 1
  obs_lines  <- 
    read_lines(sch_unzipped, skip = 2, skip_empty_rows = TRUE) %>%
    subset( !str_detect(., '^[[:space:]]*$') )
  df_lines <-
    map_dfr(obs_lines, function(l) {
      list(n_tabs       = str_count(l, '\\t'),
           possible_id  = str_match(l, '^([[:digit:]]+)\t')[2],
           raw_contents = l)
    }) %>%
    mutate(possible_id = fill_na_with_previous(.$possible_id))
  
  if (all(df_lines$n_tabs == exp_n_tabs)) {
    log_info('Lines all have expected number of tabs. Nothing to fix.')
    return(sch_unzipped)
  }
  log_info('OK. Found irregularities in number of tabs per line. Repairing!')
  
  # Some schedules have problems that need to be directly addressed, usually
  # an unwanted literal tab character in one of the free-response variables,
  # which confuses `read_tsv()` into thinking the line has an invalid number
  # of observations. If this is all that needs fixing, it's faster to just fix
  # it directly and skip the general solution below.
  
  if (str_detect(sch_unzipped, 'RIE 06302004')) {
    # This schedule has an unwanted tab character in `TEXT4468` for the bank
    # with `IDRSSD` of `490937`. It occurs in the phrase "Other[TAB]ns Exp".
    # Remove that tab using `str_replace()`
    log_info("This schedule needs a specific repair. Doing it now.")
    df_lines %<>% mutate(
      raw_contents = 
        if_else(possible_id == "490937", 
                str_replace(raw_contents, 'Other\tns Exp', 'Other ns Exp'),
                raw_contents))
    new_filename <- paste0(sch_unzipped, '.fixed')
    
    fixed_lines <- c(read_lines(sch_unzipped, n_max = 2),
                     df_lines$raw_contents)
    write_lines(fixed_lines, new_filename)
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
    log_fatal('Total tabs in file not a multiple of expected # per line, and')
    log_fatal('no specific repair has been written for it.')
    print(df_lines %>% filter(n_tabs != exp_n_tabs))
    log_info(glue(
      'Total tabs: {n_tabs_total}, # Expected Per Line: {exp_n_tabs}'))
    return(NULL)
  }
  
  log_info('Total # of tabs is multiple of expected # of tabs per line.')
  log_info('Checking if broken lines can be combined into valid lines.')
  df_lines %<>%
    group_by(possible_id) %>%
    mutate(sums_to_expected = sum(n_tabs) == exp_n_tabs)
  
  # If the total number of tabs in the schedule file is a multiple of the 
  # number of tabs expected in a valid observation line, then attempt to
  # join consecutive lines.
  
  if (all(df_lines$sums_to_expected)) {
    log_info('Consecutive broken lines have tab count summing to expected #.')
    log_info('Proceeding with repair of this schedule file...')
    good_lines <- 
      filter(df_lines, n_tabs == exp_n_tabs) %>% .$raw_contents
    fixed_lines <-
      df_lines %>%
      filter(n_tabs != exp_n_tabs) %>%
      select(possible_id, raw_contents) %>%
      summarize(fixed_line = paste0(raw_contents, collapse = '\\n')) %>%
      .$fixed_line
    header_rows <- read_lines(sch_unzipped, n_max = 2)
    sch_repaired_lines <- c(header_rows, good_lines, fixed_lines)
    sch_repaired_file  <- paste0(sch_unzipped, '.fixed')
    write_lines(sch_repaired_lines, sch_repaired_file)
    return(sch_repaired_file)
  }
  
  log_fatal("Joining broken lines won't yield rows of equal lengths, and no")
  log_fatal('specific repair has been written for this schedule file. You will')
  log_fatal('need to investigate this schedule and implement  a specific fix.')
  return(NULL)
}