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
#' @importFrom DBI dbListTables dbRemoveTable dbDisconnect
#' @importFrom purrr walk pwalk
#' @importFrom rlog log_info
#' @export
#' @examples
#' db_connector <- db_connector_sqlite('./db/ffiec.sqlite')
#' extract_all_ffiec_zips_to_db(db_connector, './zips-ffiec')
extract_all_ffiec_zips <- 
  function(db_connector, ffiec_zip_path, overwrite = FALSE) {
    if (!dir.exists('./logs')) dir.create('./logs')
    
    dttm_str <- 
      str_remove_all(Sys.time(), '[-:]') %>% 
      str_replace_all('\\s', '_')
    log_filename <- glue('./logs/extract_ffiec_{dttm_str}.log.txt')
    
    ffiec_zips <- list_ffiec_zips('~/data/callreports/ffiec')
    date_table <- tibble(ID          = 1:length(ffiec_zips),
                         REPORT_DATE = extract_ffiec_datestr(ffiec_zips))
    
    list_ffiec_zips_and_schedules(ffiec_zip_path) %>%
      pwalk(function(zip_file, sch_file) {
        callReports::extract_ffiec_schedule(db_connector, zip_file, sch_file)
      })
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
#' @importFrom dplyr arrange
#' @importFrom purrr map_chr
#' @importFrom tibble tibble
#' @export
#' @examples
#' list_ffiec_zips('./zips-ffiec')
list_ffiec_zips <- function(ffiec_zip_path) {
  rx_pattern <- '^FFIEC CDR Call Bulk All Schedules [[:digit:]]{8}\\.zip$'
  list.files(ffiec_zip_path, pattern = rx_pattern) %>%
    map_dfr(~ tibble(rep_date = extract_ffiec_datestr(.),
                     filename = paste0(ffiec_zip_path, '/', .))) %>%
    arrange(rep_date) %>%
    getElement('filename')
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
#' @importFrom glue glue
#' @importFrom rlog log_info
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
  df_summ  <- tibble(REPORT_DATE   = report_date,
                     SCHEDULE_CODE = sch_code,
                     PART_NUM      = sch_info[3],
                     PART_OF       = sch_info[4],
                     N_OBS         = ifelse(is.null(df_obs), 0, nrow(df_obs)))
  
  callReports::write_ffiec_schedule(db_connector, sch_code, df_obs, df_codes, df_summ)
  unlink(sch_unzipped)
}

#' Write extracted FFIEC schedule data to a database
#'
#' `write_ffiec_schedule()` writes the observation, codebook, and summary data
#' generated by `extract_ffiec_schedule()` to the database whose connector is
#' given by `db_connector`.
#'
#' Uses a database "transaction" to write all three tables worth of data in one
#' go and only save the data if all of it succeeds. Either everything is written
#' to the database successfully, or nothing is. This prevents duplicate data
#' being entered into the database in case one reattempts to enter a schedule
#' after a hypothetical failed attempt that gets interrupted mid-write. Using a
#' transaction allows us to respond to any failure with an error by "rolling
#' back" (undoing) any new writes to the database and returning. If successful,
#' however, we "commit" (finalize) the changes to the database so that they'll
#' be visible in future queries.
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param tbl_name The name of the table you're writing, which should be a
#' valid schedule code.
#' @param df_obs A `tibble` containing the observations found in the extracted
#' schedule file, pivoted to long form.
#' `VALUE`)
#' @param df_codes A `tibble` containing the codebook information associated
#' with the extracted schedule file.
#' @param df_summ A `tibble` containing information about how many `IDRSSD` 
#' values are associated with each `VAR_NAME` pair in the schedule
#' @importFrom DBI dbExecute dbExistsTable dbCreateTable dbWriteTable
#' @importFrom DBI dbBegin dbCommit dbRollback
#' @importFrom glue glue
#' @importFrom rlog log_info log_fatal
#' @export
write_ffiec_schedule <- 
  function(db_connector, tbl_name, df_obs, df_codes, df_summ) {
    db_conn <- db_connector()
    dbBegin(db_conn)
    tryCatch({
      dbWriteTable(db_conn, 'CODEBOOK', df_codes, append = TRUE)
      log_info(glue('Writing {nrow(df_obs)} observations to the database...'))
      if (!dbExistsTable(db_conn, tbl_name)) {
        # `IDRSSD` and `QUARTER_ID` can both be interpreted as integers, so
        # force the database to acknowledge them as such.
        dbCreateTable(conn   = db_conn, 
                      name   = tbl_name, 
                      fields = c(IDRSSD     = 'INTEGER',
                                 QUARTER_ID = 'INTEGER',
                                 VAR_NAME   = 'TEXT',
                                 VALUE      = 'TEXT'))
      }
      dbWriteTable(db_conn, tbl_name, df_obs, append = TRUE)
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
#' @export
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
#' @importFrom magrittr %>%
#' @importFrom dplyr mutate select
#' @importFrom rlog log_info
#' @importFrom tidyr pivot_longer
#' @export
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
    df_obs <- callReports::parse_ffiec_obs(sch_unzipped),
    warning = function(w) {
      log_info('Warning issued trying to extract. Running repair function...')
      sch_fixed <- callReports::fix_broken_ffiec_obs(sch_unzipped)
      tryCatch(
        df_obs <- callReports::parse_ffiec_obs(sch_fixed),
        warning = function(w) {
          log_info('Warning still issued after repairing. You may wish to')
          log_info(glue('investigate table {sch_code} for {report_date}.'))
          df_obs <- suppressWarnings(callReports::parse_ffiec_obs(sch_fixed))
        }
      )
    })
  
  if (ncol(df_obs) < 2) {
    log_info('No variables to extract. Moving on...')
    return(tibble(IDRSSD     = as.integer(NULL),
                  QUARTER_ID = as.integer(NULL),
                  VAR_NAME   = as.character(NULL),
                  VALUE      = as.character(NULL)))
  }
  
  df_obs %>%
    pivot_longer(names_to  = 'VAR_NAME',
                 values_to = 'VALUE',
                 cols = !matches('IDRSSD'),
                 values_drop_na = TRUE) %>%
    mutate(IDRSSD     = as.integer(IDRSSD),
           QUARTER_ID = as.integer(ffiec_date_str_to_qtr_id(report_date))) %>%
    select(IDRSSD, QUARTER_ID, everything())
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
#' @export
#' @examples 
#' parse_ffiec_obs('FFIEC CDR Call Schedule RCCII 06302002.txt')
parse_ffiec_obs <- function(sch_unzipped) {
  sch_file <- basename(sch_unzipped)
  log_info(glue('Parsing observations in {sch_file}'))
  
  old_coltype_option <- getOption('readr.show_col_types')
  options(readr.show_col_types = FALSE)
  df_out <-
    read_tsv(sch_unzipped,
             skip = 2,
             na = c('', 'NA', 'NR', 'CONF'),
             col_names  = extract_ffiec_names(sch_unzipped),
             col_types  = cols(.default = col_character()),
             name_repair = 
               ~ map2_chr(.x, 1:length(.x),
                          ~ ifelse(..1 == '', paste0('NONAME_', ..2), ..1)),
             col_select = !matches('NONAME_'),
             progress   = FALSE) %>%
    rename(IDRSSD = `"IDRSSD"`) %>%
    select(!any_of('RCON9999')) %>%
    mutate(IDRSSD = as.character(IDRSSD))
  options(readr.show_col_types = old_coltype_option)
  return(df_out) 
}