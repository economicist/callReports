#' Extract an entire directory of FFIEC ZIP files to a database
#'
#' `extract_all_ffiec_zips_to_db()` examines all ZIP files in `ffiec_zip_path`
#' and attempts to extract each enclosed schedule file to the database whose
#' connector function is given by `db_connector`. The underlying extraction
#' function skips schedule files that have already been successfully extracted
#' to the database.
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param ffiec_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' @return NULL
#' @importFrom magrittr %>%
#' @importFrom purrr pwalk
#' @export
#' @examples
#' db_connector <- db_connector_sqlite('./db/ffiec.sqlite')
#' extract_all_ffiec_zips_to_db(db_connector, './zips-ffiec')
extract_all_ffiec_zips <- function(db_connector, ffiec_zip_path) {
  if (!dir.exists('./logs')) dir.create('./logs')
  
  dttm_str <- 
    str_remove_all(Sys.time(), '[-:]') %>% 
    str_replace_all('\\s', '_')
  log_filename <- glue('./logs/extract_ffiec_{dttrm_str}.log.txt')
  
  sink(NULL) # Stop any logging in this session so we can start again.
  sink(log_filename, split = TRUE)
  list_ffiec_zips_and_schedules(ffiec_zip_path) %>%
    pwalk(function(zip_file, sch_file) {
      callReports::extract_ffiec_schedule(ffiec_db, zip_file, sch_file)
    })
  sink(NULL)
}

#' List ZIP and schedule files containing FFIEC bulk call report data
#' 
#' `list_ffiec_zips_and_schedules()` provides an easiy traversable data frame
#' full of zip-schedule pairs that can be used by `extract_ffiec_schedule()`
#' 
#' ZIP files containing the bulk data 2001-present offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#'
#' @param ffiec_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' @return A `tibble` containing the full paths of valid ZIP files in one column
#' paired with the schedule files contained therein in the other
#' @importFrom magrittr %>%
#' @importFrom purrr map_dfr
#' @importFrom tibble tibble
#' @export
#' @examples
#' list_ffiec_zips_and_schedules('./zips-ffiec')
list_ffiec_zips_and_schedules <- function(ffiec_zip_path) {
  list_ffiec_zips(ffiec_zip_path) %>%
    map_dfr(function(zip_file) {
      tibble(zip_file = zip_file,
             sch_file = list_ffiec_schedules(zip_file))
    })
  }

#' List ZIP files containing Call Report data from the FFIEC
#' 
#' `list_ffiec_zips()` lists valid ZIP files for extraction in a given directory.
#' 
#' ZIP files containing the bulk data 2001-present offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#'
#' @param ffiec_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' @return A character vector containing the full paths of valid ZIP files
#' @importFrom magrittr %>%
#' @export
#' @examples
#' list_ffiec_zips('./zips-ffiec')
list_ffiec_zips <- function(ffiec_zip_path) {
  list.files(ffiec_zip_path,
             pattern = 
               '^FFIEC CDR Call Bulk All Schedules [[:digit:]]{8}\\.zip$') %>%
    sapply(function(filename) paste0(ffiec_zip_path, '/', filename))
}

#' List schedule files within an FFIEC Call Report ZIP
#' 
#' `list_ffiec_schedules()` lists the schedule files contained within a given
#' ZIP file provided by the FFIEC.
#'
#' ZIP files containing the bulk data 2001-present offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#'
#' @param zip_file A ZIP file containing bulk data provided by the FFIEC
#' @return A `tibble` with the name of  containing the schedule filenames contained in `zip_file`
#' @importFrom magrittr %>%
#' @importFrom stringr str_detect
#' @export
#' @examples
#' list_ffiec_schedules('FFIEC CDR Call Bulk All Schedules 06302004.zip')
list_ffiec_schedules <- function(zip_file) {
  rx <- paste0('FFIEC CDR Call Schedule [[:alpha:]]+ [[:digit:]]{8}',
               '(\\([[:digit:]] of [[:digit:]]\\))*\\.txt')
  unzip(zip_file, list = TRUE) %>% getElement('Name') %>%
    subset(str_detect(., rx)) %>% subset(!str_detect(., 'CI|GCI|GI|GL'))
}

#' Extract the FFIEC codebook and all observations for a single schedule
#' 
#' `extract_ffiec_schedule()` accepts a ZIP file, extracts one of its enclosed
#' tab-separated-value schedule files, and writes the extracted data to a given
#' database.
#' 
#' ZIP files containing the bulk data 2001-present offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#' 
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param zf A ZIP file containing schedule files
#' @param sch A valid FFIEC schedule data file inside the ZIP file `zf`
#' @return NULL
#' @importFrom DBI dbBegin dbWriteTable dbCommit dbRollback
#' @importFrom glue glue
#' @importFrom rlog log_info log_warn
#' @export
#' @examples
#' The database connector only needs to be created once in any given script.
#' ffiec_db <- sqlite_connector('./db/ffiec.sqlite')
#' extract_ffiec_schedule(
#'   ffiec_db,
#'   './zips-ffiec/FFIEC FFIEC CDR Call Bulk All Schedules 03312018.zip',
#'   'FFIEC CDR Call Schedule RCCII 06302002.txt'
#' )
extract_ffiec_schedule <- function(db_connector, zf, sch) {
  # Unzip the schedule file and get some basic metadata for the codebook
  unzip(zf, sch, exdir = tempdir(), unzip = getOption('unzip'))
  sch_unzipped <- paste0(tempdir(), '/', sch)
  sch_file     <- basename(sch_unzipped)
  
  sch_info     <- callReports::schedule_name_components(sch_file)
  report_date  <- sch_info['report_date']
  sch_code     <- sch_info['sch_code']
  part_str     <- 
    ifelse(sch_info[4] != "1", glue('({sch_info[3]} of {sch_info[4]})'), '')
  log_info(glue('{report_date} {sch_code} {part_str}'))
  
  # Connect to the database
  db_conn <- db_connector()

    # Move on if we've already extracted this one. A schedule file's codebook
  # record is written to the database only if the respective observations are 
  # successfully extracted and written. We can thus quickly determine whether
  # the schedule is already in the database, without a full query of the
  # observation table, by consulting the codebook.
  scheds_in_db <- ffiec_schedules_in_db(db_connector)
  db_not_empty <- nrow(scheds_in_db) > 0

  if (db_not_empty) {
    matching_sch_in_db <- filter(scheds_in_db,
                                 REPORT_DATE   == report_date,
                                 SCHEDULE_CODE == sch_code,
                                 PART_NUM      == sch_info['part_num'],
                                 PART_OF       == sch_info['part_of'])
    if (nrow(matching_sch_in_db) == 1) {
      log_info('Schedule already in database. Moving to next...')
      cat('\n')
      return()
    }
  }
  
  # If you've gotten this far in the function, the database has no records
  # associated with this schedule file. Attempt to extract the observations
  # and codebook information into the database. If a warning is issued that
  # is not explicitly absorbed and reconciled by the observation extraction
  # function, a visible warning log will be shown here.
  df_codes <- callReports::extract_ffiec_codebook(sch_unzipped)
  df_obs   <- callReports::extract_ffiec_obs(sch_unzipped)
  n_obs    <- nrow(df_obs)
  df_summ  <- tibble(REPORT_DATE   = report_date,
                     SCHEDULE_CODE = sch_code,
                     PART_NUM      = sch_info[3],
                     PART_OF       = sch_info[4],
                     N_OBS         = n_obs)
  
  # Use a database "transaction" to write the codebook and observations in one
  # go. If either fails with an error, "rollback" (undo) any new writes to the
  # database and return. Otherwise, "commit" the new records to the database.
  # Disconnect when done either way.
  dbBegin(db_conn)
  tryCatch({
    dbWriteTable(db_conn, 'CODEBOOK', df_codes, append = TRUE)
    log_info(glue('Writing {n_obs} observations to the database...'))
    dbWriteTable(db_conn, sch_code, df_obs, append = TRUE)
    dbWriteTable(db_conn, 'SUMMARY', df_summ, append = TRUE)
    dbCommit(db_conn)
  },
  warning = function(w) {
    warning(w)
  },
  error = function(e) {
    dbRollback(db_conn)
    log_fatal('Error writing data to the database. No observations added:')
    stop(e)
  },
  finally = {
    dbDisconnect(db_conn)
    unlink(sch_unzipped)
    cat('\n')
  })
}

#' Extract the FFIEC codebook information for a given schedule file
#'
#' `extract_ffiec_codebook()` accepts a text file containing tab-separated-value
#' bulk data provided by the FFIEC, and writes the variable names, their 
#' descriptions, and the table where they can be found to a given database.
#'
#' Most applications will not use this function as it is called by
#' `extract_ffiec_schedule()` as part of extracting the codebook and observations
#' simultaneously.
#'
#' ZIP files containing valid schedule files for 2001-present are offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#' 
#' @param sch_unzipped The path to a valid FFIEC schedule data file on disk
#' @return NULL
#' @importFrom stringr str_extract
#' @importFrom tibble tibble
#' @examples
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- db_connector_sqlite('./db/ffiec.sqlite')
#' extract_ffiec_codebook(ffiec_db, 'FFIEC CDR Call Schedule RCCII 06302002.txt')
extract_ffiec_codebook <- function(sch_unzipped) {
  sch_file <- basename(sch_unzipped)
  sch_code <- callReports::extract_schedule_code(sch_file)
  
  log_info(glue('Extracting codebook...'))
  report_date <- callReports::extract_ffiec_datestr(sch_unzipped)
  sch_code    <- callReports::extract_schedule_code(sch_unzipped)
  part_code   <- callReports::extract_part_codes(sch_unzipped)
  
  tibble(
    REPORT_DATE   = report_date,
    SCHEDULE_CODE = sch_code,
    PART_NUM      = part_code[1],
    PART_OF       = part_code[2],
    VAR_NAME      = extract_ffiec_names(sch_unzipped),
    VAR_DESC      = extract_ffiec_descs(sch_unzipped)
  ) %>% filter(str_length(VAR_DESC) != 0)
}

#' Extract the observations for a given FFIEC schedule file to a database
#'
#' `extract_ffiec_obs()` accepts a text file containing tab-separated-value
#' bulk data provided by the FFIEC, and writes it to a given database.
#' 
#' If any warnings are triggered about parsing issues, they are absorbed by the
#' function and an attempt is made to repair the schedule file. A second attempt
#' is then made, and a visible warning issued it was not resolved. Any data
#' extracted will still be written to the database, but you may wish to
#' investigate their integrity. Tests existing data sets as of 2022 June 17
#' have been working without warnings.
#'
#' Most applications will not use this function as it is called by
#' `extract_ffiec_schedule()` as part of extracting the codebook and observations
#' simultaneously.
#'
#' ZIP files containing valid schedule files for 2001-present are offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#' 
#' @param sch_unzipped A valid FFIEC schedule data file extracted to disk#'
#' @return NULL
#' @importFrom magrittr %>%
#' @importFrom dplyr mutate select
#' @importFrom rlog log_info log_warn
#' @importFrom tidyr pivot_longer
#' @examples 
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- db_connector_sqlite('./db/ffiec.sqlite')
#' extract_ffiec_obs(ffiec_db, 'FFIEC CDR Call Schedule RCCII 06302002.txt')
extract_ffiec_obs <- function(sch_unzipped) {
  sch_file    <- basename(sch_unzipped)
  sch_code    <- callReports::extract_schedule_code(sch_unzipped)
  report_date <- callReports::extract_ffiec_datestr(sch_unzipped)
  log_info(glue('Extracting observations...'))
  
  df_obs <- tryCatch(
    callReports::parse_ffiec_obs(sch_unzipped),
    warning = function(w) {
      log_warn('Warning issued trying to extract. Running repair function...')
      sch_fixed <- callReports::fix_broken_ffiec_obs(sch_unzipped)
      if (sch_fixed == sch_unzipped) {
        log_warn('Repair function found no unwanted newlines or tabs.')
        log_warn('You may wish to investigate both the original input schedule')
        log_warn(
          glue('file {sch_unzipped} and the extracted output in the database'))
        log_warn(
          glue('anomalies in table {sch_code} records for {report_date}.'))
      } 
      parse_ffiec_obs(sch_fixed)
    })
  
  if (ncol(df_obs) < 2) {
    log_info('No variables to extract. Moving on...')
    return(NULL)
  }
  
  df_obs %>%
    pivot_longer(names_to  = 'VAR_NAME',
                 values_to = 'VALUE',
                 cols = !matches('IDRSSD'),
                 values_drop_na = TRUE) %>%
    mutate(REPORT_DATE = report_date) %>%
    select(IDRSSD, REPORT_DATE, everything())
}

#' Parse the observations in a given FFIEC schedule file for processing in `R`
#'
#' `parse_ffiec_obs()` accepts a text file containing tab-separated-value
#' bulk data provided by the FFIEC, and parses them into an object that can
#' be manipulated in `R`.
#'
#' Each valid line in the schedule ends in a blank tab-separated value that has
#' no name in the header. This triggers a message about giving that column a
#' name when it is parsed. That message is suppressed by explicitly providing a
#' method for assigning a name to that blank column. That column is deselected
#' before the data frame is returned.
#'
#' This function is unlikely to ever be called by the user except for diagnostics.
#'
#' ZIP files containing valid schedule files for 2001-present are offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#' 
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param sch_unzipped A valid FFIEC schedule data file extracted to disk#'
#' @return A `tibble` containing the schedule data in wide form
#' @importFrom magrittr %>%
#' @importFrom dplyr rename select
#' @importFrom purrr map2_chr
#' @importFrom readr cols col_character read_tsv
#' @importFrom rlog log_info
#' @examples 
#' parse_ffiec_obs('FFIEC CDR Call Schedule RCCII 06302002.txt')
parse_ffiec_obs <- function(sch_unzipped) {
  sch_file <- basename(sch_unzipped)
  log_info(glue('Parsing observations in {sch_file}'))
  read_tsv(sch_unzipped,
           skip = 2,
           na = c('', 'NA', 'NR', 'CONF'),
           col_names  = extract_ffiec_names(sch_unzipped),
           col_types  = cols(.default = col_character()),
           name_repair = function(nms) {
             map2_chr(nms, 1:length(nms), function(nm, idx) {
               ifelse(nm == '', paste0('NONAME_', idx), nm)
             })
           },
           col_select = !matches('NONAME_'),
           progress   = FALSE) %>%
    rename(IDRSSD = `"IDRSSD"`) %>%
    select(!any_of('RCON9999')) %>%
    mutate(IDRSSD = as.character(IDRSSD))
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
  part_codes <- callReports::extract_part_codes(sch)
  return(c(sch_info, part_codes))
}

#' Extract the variable names from an FFIEC schedule file
#'
#' @param sch_unzipped The path to an FFIEC schedule file on disk.
#' @return A character vector containing the variable names found in the top line.
#' @importFrom readr read_lines
#' @examples
#' extract_ffiec_names('FFIEC CDR Call Schedule RCCII 06302002.txt')
extract_ffiec_names <- function(sch_unzipped) { 
  read_lines(sch_unzipped, n_max = 1, progress = FALSE) %>% 
    callReports::str_split1('\\t')
}

#' Extract the variable descriptions from an FFIEC schedule file
#'
#' @param sch_unzipped The path to an FFIEC schedule file on disk.
#' @return A character vector containing the variable descriptions found in the 
#' second line of the file.
#' @importFrom readr read_lines
#' @examples
#' extract_ffiec_descs('FFIEC CDR Call Schedule RCCII 06302002.txt')
extract_ffiec_descs <- function(sch_unzipped) { 
  read_lines(sch_unzipped, skip = 1, n_max = 1, progress = FALSE) %>%
    callReports::str_split1('\\t')
}

#' Extract the date from an FFIEC schedule filename in `YYYY-MM-DD` format
#'
#' @param sch The name of an FFIEC schedule file, existent or not.
#' @return A character value in `YYYY-MM-DD` format containing the date of the
#' report corresponding to that schedule filename.
#' @importFrom magrittr %>%
#' @importFrom lubridate mdy
#' @importFrom stringr str_extract
#' @examples
#' extract_ffiec_datestr('FFIEC CDR Call Schedule RCCII 06302002.txt')
extract_ffiec_datestr <- function(sch) {
  str_extract(sch, '[[:digit:]]{8}') %>% mdy() %>% as.character()
}

#' Extract the alphabetical schedule code from an FFIEC schedule filename
#'
#' @param sch The name of an FFIEC schedule file, existent or not.
#' @return A character value containing the alphabetical schedule code 
#' corresponding to the given schedule filename.
#' @importFrom stringr str_match
#' @examples
#' > extract_schedule_code('FFIEC CDR Call Schedule RCCII 06302002.txt')
#' [1] "RCCII"
extract_schedule_code <- function(sch) {
  str_match(sch, '([[:alpha:]]+) [[:digit:]]{8}')[1, 2]
}

#' Extract the schedule part number from an FFIEC schedule filename
#'
#' If data for a schedule is issued in one part (true for most), then it will
#' be determined to be part 1 of 1.
#'
#' @param sch The name of an FFIEC schedule file, existent or not.
#' @return A numeric vector with two named values: `part_num` and `part_of`,
#' with both being `1` if no part is indicated in the filename.
#' 
#' @importFrom stringr str_match
#' @examples
#' > extract_part_code('FFIEC CDR Call Schedule RCB 03312013(2 of 2).txt')
#' part_num  part_of 
#'        1        2 
extract_part_codes <- function(sch) {
  rx <- '([[:digit:]]{8})(\\(([[:digit:]]) of ([[:digit:]])\\))*'
  part_codes <- str_match(sch, rx)
  if (is.na(part_codes[1, 3])) return(c(part_num = 1, part_of = 1))
  return(c(part_num = part_codes[1, 4], part_of = part_codes[1, 5]))
}

#' Extract the values from one line of an FFIEC schedule file
#' 
#' @param sch_line A line from an FFIEC tab-separated schedule file
#' @return A character vector of the values in that line
#' @examples
#' extract_values('12311  2031  298310')
#' [1] "12311" "2031" "298310"
extract_values <- function(sch_line) {
  sch_line %>% callReports::str_split1('\\t')
}