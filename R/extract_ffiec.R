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
#' @export
#' @examples
#' db_connector <- db_connector_sqlite('./db/ffiec.sqlite')
#' extract_all_ffiec_zips_to_db(db_connector, './zips-ffiec')
extract_all_ffiec_zips <- 
  function(db_connector, ffiec_zip_path, overwrite = FALSE) {
    if (!dir.exists('./logs')) dir.create('./logs')
    
    dttm_str <- 
      stringr::str_remove_all(Sys.time(), '[-:]') %>% 
      stringr::str_replace_all('\\s', '_')
    log_filename <- glue::glue('./logs/extract_ffiec_{dttm_str}.log.txt')
    
    ffiec_zips <- list_ffiec_zips('~/data/callreports/ffiec')
    date_table <- tibble::tibble(ID          = 1:length(ffiec_zips),
                                 REPORT_DATE = extract_ffiec_datestr(ffiec_zips))
    
    list_ffiec_zips_and_schedules(ffiec_zip_path) %>%
      purrr::pwalk(function(zip_file, sch_file) {
        process_ffiec_schedule(db_connector, zip_file, sch_file)
      })
  }

#' List ZIP and schedule files containing FFIEC bulk call report data
#' 
#' `list_ffiec_zips_and_schedules()` provides an easiy traversable data frame
#' full of zip-schedule pairs that can be used by `process_ffiec_schedule()`
#' 
#' ZIP files containing the bulk data 2001-present offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#'
#' @param ffiec_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' @return A `tibble` containing the full paths of valid ZIP files in one column
#' paired with the schedule files contained therein in the other
#' @export
#' @examples
#' list_ffiec_zips_and_schedules('./zips-ffiec')
list_ffiec_zips_and_schedules <- function(ffiec_zip_path) {
  list_ffiec_zips(ffiec_zip_path) %>%
    purrr::map_dfr(function(zip_file) {
      tibble::tibble(zip_file = zip_file,
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
#' @export
#' @examples
#' list_ffiec_zips('./zips-ffiec')
list_ffiec_zips <- function(ffiec_zip_path) {
  rx_pattern <- '^FFIEC CDR Call Bulk All Schedules [[:digit:]]{8}\\.zip$'
  list.files(ffiec_zip_path, pattern = rx_pattern) %>%
    purrr::map_dfr(~ tibble::tibble(rep_date = extract_ffiec_datestr(.),
                     filename = paste0(ffiec_zip_path, '/', .))) %>%
    dplyr::arrange(rep_date) %>%
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
#' @export
#' @examples
#' list_ffiec_schedules('FFIEC CDR Call Bulk All Schedules 06302004.zip')
list_ffiec_schedules <- function(zip_file) {
  rx <- paste0('FFIEC CDR Call Schedule [[:alpha:]]+ [[:digit:]]{8}',
               '(\\([[:digit:]] of [[:digit:]]\\))*\\.txt')
  unzip(zip_file, list = TRUE) %>% getElement('Name') %>%
    subset(stringr::str_detect(., rx)) %>% subset(!stringr::str_detect(., 'CI|GCI|GI|GL'))
}

#' Extract the FFIEC codebook and all observations for a single schedule
#' 
#' `process_ffiec_schedule()` accepts a ZIP file, extracts one of its enclosed
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
#' @export
#' @examples
#' The database connector only needs to be created once in any given script.
#' ffiec_db <- sqlite_connector('./db/ffiec.sqlite')
#' process_ffiec_schedule(
#'   ffiec_db,
#'   './zips-ffiec/FFIEC FFIEC CDR Call Bulk All Schedules 03312018.zip',
#'   'FFIEC CDR Call Schedule RCCII 06302002.txt'
#' )
process_ffiec_schedule <- function(db_connector, zf, sch) {
  # Unzip the schedule file and get some basic metadata for the codebook
  unzip(zf, sch, exdir = tempdir(), unzip = getOption('unzip'))
  sch_unzipped <- paste0(tempdir(), '/', sch)
  sch_file     <- basename(sch_unzipped)
  
  sch_info     <- schedule_name_components(sch_file)
  report_date  <- sch_info['report_date']
  sch_code     <- sch_info['sch_code']
  part_str     <- 
    ifelse(sch_info[4] != "1", glue::glue('({sch_info[3]} of {sch_info[4]})'), '')
  rlog::log_info(glue::glue('{report_date} {sch_code} {part_str}'))
  
  # Move on if we've already extracted this one. A schedule file's codebook
  # record is written to the database only if the respective observations are 
  # successfully extracted and written. We can thus quickly determine whether
  # the schedule is already in the database, without a full query of the
  # observation table, by consulting the codebook.
  scheds_in_db <- ffiec_schedules_in_db(db_connector)
  db_not_empty <- nrow(scheds_in_db) > 0

  if (db_not_empty) {
    matching_sch_in_db <- dplyr::filter(scheds_in_db,
                                 REPORT_DATE   == report_date,
                                 SCHEDULE_CODE == sch_code,
                                 PART_NUM      == sch_info['part_num'],
                                 PART_OF       == sch_info['part_of'])
    if (nrow(matching_sch_in_db) == 1) {
      rlog::log_info('Schedule already in database. Moving to next...')
      cat('\n')
      return()
    }
  }
  
  # If you've gotten this far in the function, the database has no records
  # associated with this schedule file. Attempt to extract the observations
  # and codebook information. Once it's extracted, take the variable names and
  # add them to the `VAR_CODES` table in the database, where each VAR_CODE will
  # be assigned a simple integer value for more efficient storage in the main
  # observation tables.
  df_codes  <- extract_ffiec_codebook(sch_unzipped)
  var_codes <- unique(df_codes$VAR_CODE)
  write_var_codes(db_connector, var_codes)
  
  # Attempt to extract the observations for this file. If a warning is issued 
  # that is not explicitly absorbed and reconciled by the observation extraction
  # function, a visible warning log will be shown here.
  db_conn <- db_connector()
  df_varcodes <- 
    DBI::dbReadTable(db_conn, 'VAR_CODES') %>%
    dplyr::filter(VAR_CODE %in% var_codes) %>%
    dplyr::collect()
  df_obs <- 
    extract_ffiec_schedule(sch_unzipped) %>%
    tidyr::pivot_longer(names_to  = 'VAR_CODE',
                 values_to = 'VALUE',
                 cols = !any_of(c('IDRSSD')),
                 values_drop_na = TRUE) %>%
    dplyr::mutate(IDRSSD     = as.integer(IDRSSD),
           QUARTER_ID = as.integer(date_str_to_qtr_id(report_date))) %>%
    dplyr::inner_join(df_varcodes, by = 'VAR_CODE') %>%
    dplyr::rename(VAR_CODE_ID = ID) %>%
    dplyr::select(IDRSSD, QUARTER_ID, VAR_CODE_ID, VALUE)
  df_summ  <- 
    tibble::tibble(REPORT_DATE   = report_date,
                   SCHEDULE_CODE = sch_code,
                   PART_NUM      = sch_info[3],
                   PART_OF       = sch_info[4],
                   N_OBS         = ifelse(is.null(df_obs), 0, nrow(df_obs)))
  
  write_ffiec_schedule(db_connector, sch_code, df_obs, df_codes, df_summ)
  unlink(sch_unzipped)
}

#' Extract the observations for a given FFIEC schedule file to a database
#'
#' `extract_ffiec_schedule()` accepts a text file containing tab-separated-value
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
#' `process_ffiec_schedule()` as part of extracting the codebook and observations
#' simultaneously.
#'
#' ZIP files containing valid schedule files for 2001-present are offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#' 
#' @param sch_unzipped A valid FFIEC schedule data file extracted to disk#'
#' @export
#' @examples 
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- db_connector_sqlite('./db/ffiec.sqlite')
#' extract_ffiec_schedule(ffiec_db, 'FFIEC CDR Call Schedule RCCII 06302002.txt')
extract_ffiec_schedule <- function(sch_unzipped) {
  sch_file    <- basename(sch_unzipped)
  sch_code    <- extract_schedule_code(sch_unzipped)
  report_date <- extract_ffiec_datestr(sch_unzipped)
  rlog::log_info(glue::glue('Extracting observations...'))
  
  df_obs <- tryCatch(
    df_obs <- parse_ffiec_schedule(sch_unzipped),
    warning = function(w) {
      rlog::log_info('Warning issued trying to extract. Running repair function...')
      sch_fixed <- fix_broken_ffiec_obs(sch_unzipped)
      tryCatch(
        df_obs <- parse_ffiec_schedule(sch_fixed),
        warning = function(w) {
          rlog::log_info('Warning still issued after repairing. You may wish to')
          rlog::log_info(glue::glue('investigate table {sch_code} for {report_date}.'))
          df_obs <- suppressWarnings(parse_ffiec_schedule(sch_fixed))
        }
      )
    })
  
  return(df_obs)
}

#' Parse the observations in a given FFIEC schedule file for processing in `R`
#'
#' `parse_ffiec_schedule()` accepts a text file containing tab-separated-value
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
#' @export
#' @examples 
#' parse_ffiec_schedule('FFIEC CDR Call Schedule RCCII 06302002.txt')
parse_ffiec_schedule <- function(sch_unzipped) {
  sch_file <- basename(sch_unzipped)
  rlog::log_info(glue::glue('Parsing observations in {sch_file}'))
  
  old_coltype_option <- getOption('readr.show_col_types')
  options(readr.show_col_types = FALSE)
  df_out <-
    readr::read_tsv(sch_unzipped,
                    skip = 2,
                    na = c('', 'NA', 'NR', 'CONF'),
                    col_names  = extract_ffiec_names(sch_unzipped),
                    col_types  = readr::cols(.default = readr::col_character()),
                    name_repair = 
                      ~ purrr::map2_chr(.x, 1:length(.x),
                                        ~ ifelse(..1 == '', paste0('NONAME_', ..2), ..1)),
                    col_select = !matches('NONAME_'),
                    progress   = FALSE) %>%
    dplyr::rename(IDRSSD = `"IDRSSD"`) %>%
    dplyr::select(!any_of('RCON9999')) %>%
    dplyr::mutate(IDRSSD = as.character(IDRSSD))
  options(readr.show_col_types = old_coltype_option)
  return(df_out) 
}



#' Extract the FFIEC codebook information for a given schedule file
#'
#' `extract_ffiec_codebook()` accepts a text file containing tab-separated-value
#' bulk data provided by the FFIEC, and writes the variable names, their 
#' descriptions, and the table where they can be found to a given database.
#'
#' Most applications will not use this function as it is called by
#' `process_ffiec_schedule()` as part of extracting the codebook and observations
#' simultaneously.
#'
#' ZIP files containing valid schedule files for 2001-present are offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#' 
#' @param sch_unzipped The path to a valid FFIEC schedule data file on disk
#' @return NULL
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- db_connector_sqlite('./db/ffiec.sqlite')
#' extract_ffiec_codebook(ffiec_db, 'FFIEC CDR Call Schedule RCCII 06302002.txt')
extract_ffiec_codebook <- function(sch_unzipped) {
  sch_file <- basename(sch_unzipped)
  sch_code <- extract_schedule_code(sch_file)
  
  rlog::log_info(glue::glue('Extracting codebook...'))
  report_date <- extract_ffiec_datestr(sch_unzipped)
  sch_code    <- extract_schedule_code(sch_unzipped)
  part_code   <- extract_part_codes(sch_unzipped)
  
  tibble::tibble(
    REPORT_DATE   = report_date,
    SCHEDULE_CODE = sch_code,
    PART_NUM      = part_code[1],
    PART_OF       = part_code[2],
    VAR_CODE      = extract_ffiec_names(sch_unzipped),
    VAR_DESC      = extract_ffiec_descs(sch_unzipped)
  ) %>% dplyr::filter(stringr::str_length(VAR_DESC) != 0)
}

