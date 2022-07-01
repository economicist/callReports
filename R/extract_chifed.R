#' List ZIP files containing Call Report data from the Chicago Fed
#' 
#' `list_chifed_zips()` expects the path of a folder containing the bulk data
#' files for 1976-2000 offered at the [Chicago Fed's website](https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data-complete-1976-2000) 
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @param chifed_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' @param overwrite_sqlite (default `FALSE`) Should we overwrite an existing
#' SQLite DB? This is meaningful only for SQLite databases.
#' @return A character vector containing the full paths of valid ZIP files
#' @export
#' @examples
#' extract_all_chifed_zips('./zips-chifed')
extract_all_chifed_zips <- 
  function(db_connector, chifed_zip_path) {
    `%not_in%`  <- Negate(`%in%`)
    if (!dir.exists('./logs')) dir.create('./logs')
    
    codebook_path <- glue::glue('{chifed_zip_folder}/MDRM.zip')
    extract_chifed_codebook(sqlite_connector, codebook_path)
        
    db_conn <- db_connector()
    yymm_in_db <-
      chifed_dates_in_db(db_connector) %>%
      sapply(function(dt) {
        yy <- lubridate::year(dt) %% 100
        mm <- lubridate::month(dt)
        paste0(yy, stringr::str_pad(mm, 2, 'left', '0'))
      })
    DBI::dbDisconnect(db_conn)
    
    list_all_chifed_zips(chifed_zip_path) %>%
      subset(stringr::str_extract(., '[[:digit:]]{4}') %not_in% yymm_in_db) %>%
      purrr::walk(function(zip_file) extract_chifed_zip(db_connector, zip_file))
  }

#' List ZIP files containing Call Report data from the Chicago Fed
#' 
#' `list_chifed_zips()` expects the path of a folder containing the bulk data
#' files for 1976-2000 offered at the [Chicago Fed's website](https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data-complete-1976-2000) 
#'
#' @param chifed_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' @return A character vector containing the full paths of valid ZIP files
#' @export
#' @examples
#' list_all_chifed_zips('./zips-chifed')
list_all_chifed_zips <- function(chifed_zip_path) {
  matching_zips <- 
    list.files(chifed_zip_path,
               pattern = '(CALL|call)[[:digit:]]{4}-(ZIP|zip)\\.(ZIP|zip)',
               full.names = TRUE)
  
  # Put the year 2000 zips at the end so they're in order. The year is two
  # digits so just using `sort()` won't do it.
  zips_2000  <- matching_zips[stringr::str_detect(matching_zips, '(CALL|call)00')]
  zips_other <- matching_zips[!stringr::str_detect(matching_zips, '(CALL|call)00')]
  zips_reordered <- c(zips_other, zips_2000)
  return(zips_reordered)
}

#' Extract the Chicago Fed's data observations to a database
#'
#' `extract_chifed_observations()` reads a ZIP file containing a valid XPT file.
#' 
#' ZIP files containing the data are available at the [Chicago Fed](https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data-complete-1976-2000).
#' They should be saved (but not extracted) to an empty folder.
#' 
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @param zip_file An existing ZIP file containing a valid XPT data file
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' chifed_db <- sqlite_connector('./db/chifed.sqlite')
#' extract_chifed_zip(chifed_db, './zips-chifed/call0003-zip.zip')
extract_chifed_zip <- function(db_connector, zip_file) {
  `%not_in%` <- Negate(`%in%`)
  yy     <- stringr::str_match(zip_file, '([[:digit:]]{2})[[:digit:]]{2}')[1, 2]
  yyyy   <- ifelse(yy == '00', '2000', paste0('19', yy))
  mm     <- stringr::str_match(zip_file, '[[:digit:]]{2}([[:digit:]]{2})')[1, 2]
  dt_str <- glue::glue('{yyyy}-{mm}-01')
  
  df_tbl  <- extract_chifed_obs(zip_file)
  
  rlog::log_info('Getting record count per variable for `CHIFED.SUMMARY` table...')
  df_summ <- 
    dplyr::select(df_tbl, !any_of(c('IDRSSD', 'QUARTER_ID'))) %>%
    dplyr::summarize_all(~ sum(!is.na(.x) | is.null(.x))) %>%
    tidyr::pivot_longer(cols      = everything(),
                 names_to  = 'VAR_CODE',
                 values_to = 'N_RECORDS') %>%
    dplyr::mutate(REPORT_DATE = dt_str) %>%
    dplyr::select(REPORT_DATE, VAR_CODE, 'N_RECORDS')

  # The Chicago Fed's quarterly call report data is disributed all in one table
  # each period. The table has more columns than the SQLite database backend
  # can handle without modifying and recompiling it. The quick and dirty
  # workaround is to simply pivot the data to a long form similar to what we
  # did with the data from the FFIEC. We'll rename the date and ID columns to
  # match those ones.
  
  # Get a list of the variable code in this ZIP, and send them to the function
  # that assigns an ID value to each code (for codes already in the DB, their
  # previously-assigned ID value is persistent). Then retrieve 
  new_var_codes <- 
    names(df_tbl) %>%
    subset(. %not_in% c('IDRSSD', 'QUARTER_ID'))
  write_var_codes(db_connector, new_var_codes)
  var_codes_in_db <- fetch_var_codes(db_connector)
  
  rlog::log_info('Pivoting to long form for writing to `CHIFED.OBS_ALL` table...')
  if ('CALL8787' %not_in% df_summ$VAR_CODE) df_tbl %<>% dplyr::mutate(CALL8787 = NA)
  df_out <-
    df_tbl %>%
    tidyr::pivot_longer(cols = 
                   !any_of(c('IDRSSD', 'QUARTER_ID', 'CALL8786', 'CALL8787')),
                 names_to         = 'VAR_CODE',
                 values_to        = 'VALUE',
                 values_transform = as.character,
                 values_drop_na   = TRUE) %>%
    dplyr::inner_join(var_codes_in_db, by = 'VAR_CODE') %>%
    dplyr::rename(VAR_CODE_ID = ID) %>%
    dplyr::select(IDRSSD, QUARTER_ID, CALL8786, CALL8787, VAR_CODE_ID, VALUE)
  
  num_vars      <- ncol(df_tbl) - 2 # Don't count `IDRSSD` or `QUARTERID`
  num_rows_long <- nrow(df_out)
  rlog::log_info(
    glue::glue('Writing {num_rows_long} rows of {num_vars} variables to database...'))
  
  db_conn <- db_connector()
  tryCatch({
    DBI::dbBegin(db_conn)
    if (!DBI::dbExistsTable(db_conn, 'CHIFED.OBS_ALL')) {
      DBI::dbCreateTable(db_conn, 'CHIFED.OBS_ALL',
                    fields = c(IDRSSD      = 'INTEGER',
                               QUARTER_ID  = 'INTEGER',
                               CALL8786    = 'INTEGER',
                               CALL8787    = 'INTEGER',
                               VAR_CODE_ID = 'INTEGER',
                               VALUE       = 'TEXT'))
    }
    DBI::dbWriteTable(db_conn, 'CHIFED.OBS_ALL', df_out, append = TRUE)
    DBI::dbWriteTable(db_conn, 'CHIFED.SUMMARY', df_summ, append = TRUE)
    DBI::dbCommit(db_conn)
  }, warning = stop, error = stop)
  DBI::dbDisconnect(db_conn)
  
  rlog::log_info(glue::glue('Successfully extracted {zip_file} to the database!'))
  cat('\n')
}

#' Extract the observations from a Chicago Fed ZIPped XPT file.
#'
#' ZIP files containing the data are available at the [Chicago Fed](https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data-complete-1976-2000).
#' They should be saved (but not extracted) to an empty folder.
#'
#' @param zip_file the path to a ZIP file on disk containing a valid XPT file
#' @return A `tibble` containing the observations in wide form.
#' @export
extract_chifed_obs <- function(zip_file) {
  # These are distributed in SAS XPT format, an open-source format that allows
  # SAS users to distribute data in a format compatible with regulators and
  # journals requiring open-source data. They can be easily read with the
  # `read_xpt()` function in the `haven` package.
  # 
  # Some ZIP files contain superfluous files beyond the desired XPT, and the
  # XPT file uses different capitalization by period. We need to get the correct
  # name of the enclosed file before extracting it.
  yymm_str <- stringr::str_extract(zip_file, '[[:digit:]]{4}')
  xpt_file <- 
    unzip(zip_file, list = TRUE)[['Name']] %>%
    subset(stringr::str_detect(., '\\.(XPT|xpt)'))
  
  yy     <- stringr::str_match(zip_file, '([[:digit:]]{2})[[:digit:]]{2}')[1, 2]
  yyyy   <- ifelse(yy == '00', '2000', paste0('19', yy))
  mm     <- stringr::str_match(zip_file, '[[:digit:]]{2}([[:digit:]]{2})')[1, 2]
  dt_str <- glue::glue('{yyyy}-{mm}-01')
  qtr_id <- date_str_to_qtr_id(dt_str)
  
  rlog::log_info(glue::glue('Reading observations from {xpt_file} in {zip_file}'))
  df_tbl    <- unz(zip_file, xpt_file) %>% haven::read_xpt()
  var_codes <- names(df_tbl)
  
  rlog::log_info(glue::glue(
    'Extracted {nrow(df_tbl)} rows containing {ncol(df_tbl)} columns'))
  
  # Most periods have the reporting institution listed as "ENTITY". For whatever
  # reason, this file has no formal "ENTITY" column and we need to copy it over
  # from another variable called "RSSD9001".
  if ('RSSD9001' %in% var_codes) {
    rlog::log_info('ID Column `RSSD9001` detected...')
    df_tbl %<>% dplyr::select(!any_of(c('IDRSSD', 'ENTITY')))
  } else if ('ENTITY' %in% var_codes) {
    rlog::log_info('ID Column `RSSD9001` not detected. Renaming `ENTITY`-->`RSSD9001')
    df_tbl %<>% dplyr::mutate(RSSD9001 = ENTITY) %>% dplyr::select(!any_of(c('ENTITY')))
  } else if ('IDRSSD' %in% var_codes) {
    rlog::log_info('ID Column `RSSD9001` not detected. Renaming `IDRSSD`-->`RSSD9001')
    df_tbl %<>% dplyr::mutate(RSSD9001 = IDRSSD) %>% dplyr::select(!any_of(c('IDRSSD')))
  } else {
    stop(glue::glue('No valid ID column found in {xpt_file}... Cannot go on!'))
  }
  
  rlog::log_info('Renaming `RSSD9001` to `IDRSSD` for consistency with FFIEC data.')
  df_tbl %<>%
    dplyr::mutate(IDRSSD = as.character(RSSD9001), QUARTER_ID = qtr_id) %>%
    dplyr::select(IDRSSD, QUARTER_ID, 
           !any_of(c('RSSD9999', 'RSSD9001', 'ENTITY', 'DATE')))
  rlog::log_info(glue::glue('Done parsing {xpt_file}...'))
  return(df_tbl)
}

#' Extract the Chicago Fed's codebook data to a database
#'
#' `extract_chifed_codebook()` extracts the Chicago Fed's call report data
#' dictionary provided at (their website)[https://www.federalreserve.gov/apps/mdrm/download_mdrm.htm]
#' to a database specified in the given database connector function. 
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @param codebook_zip An existing copy of `MDRM.zip`
#' page accompanying the main data.
#' @return NULL
#' @export
#' @examples
#' The database connector only needs to be created once in any given script.
#' chifed_db <- sqlite_connector('./db/chifed.sqlite')
#' extract_chifed_codebook(chifed_db, './MDRM.zip')
extract_chifed_codebook <- function(db_connector, codebook_zip) {
  codebook <- 
    unz(codebook_zip, 'MDRM_CSV.csv') %>% 
    readr::read_csv(skip = 1, col_types = readr::cols(.default = readr::col_character())) %>%
    dplyr::rename_all(function(nm) stringr::str_replace_all(toupper(nm), '\\s', '_'))
  db_conn <- db_connector()
  try(DBI::dbWriteTable(db_conn, 'CHIFED.CODEBOOK', codebook))
  DBI::dbDisconnect(db_conn)
}

#' What dates have already been extracted to the Chicago Database?
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @return A character vector containing dates of already-extracted datasets
#' in the Chicago database, in ISO 'YYYY-MM-DD' format
#' @export
chifed_dates_in_db <- function(db_connector) {
  `%not_in%` <- Negate(`%in%`)
  db_conn <- db_connector()
  if (!DBI::dbExistsTable(db_conn, 'SUMMARY')) return(NULL)
  if ('REPORT_DATE' %not_in% DBI::dbListFields(db_conn, 'SUMMARY')) return(NULL)
  dates_in_db <-
    DBI::dbReadTable(db_conn, 'SUMMARY') %>%
    dplyr::distinct(REPORT_DATE) %>% 
    dplyr::collect()
  DBI::dbDisconnect(db_conn)
  return(dates_in_db)
}
