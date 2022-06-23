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
#' @importFrom magrittr %>%
#' @importFrom glue glue
#' @importFrom lubridate month year
#' @importFrom stringr str_extract str_remove_all str_replace_all str_pad
#' @export
#' @examples
#' extract_all_chifed_zips('./zips-chifed')
extract_all_chifed_zips <- 
  function(db_connector, chifed_zip_path) {
    `%not_in%`  <- Negate(`%in%`)
    if (!dir.exists('./logs')) dir.create('./logs')
    
    dttm_str <- 
      str_remove_all(Sys.time(), '[-:]') %>% 
      str_replace_all('\\s', '_')
    log_filename <- glue('./logs/extract_chifed_{dttm_str}.log.txt')
    
    try(sink(NULL)) # Stop any logging in this session so we can start again.
    sink(log_filename, split = TRUE)
    db_conn <- db_connector()
    yymm_in_db <-
      chifed_dates_in_db(db_connector) %>%
      sapply(function(dt) {
        yy <- year(dt) %% 100
        mm <- month(dt)
        paste0(yy, str_pad(mm, 2, 'left', '0'))
      })
    list_all_chifed_zips(chifed_zip_path) %>%
      subset(str_extract(., '[[:digit:]]{4}') %not_in% yymm_in_db) %>%
      walk(function(zip_file) extract_chifed_zip(db_connector, zip_file))
    sink(NULL)
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
  zips_2000  <- matching_zips[str_detect(matching_zips, '(CALL|call)00')]
  zips_other <- matching_zips[!str_detect(matching_zips, '(CALL|call)00')]
  zips_reordered <- c(zips_other, zips_2000)
  return(zips_reordered)
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
#' @importFrom DBI dbWriteTable dbDisconnect
#' @importFrom dplyr rename_all
#' @importFrom readr cols read_csv
#' @importFrom stringr str_replace_all
#' @export
#' @examples
#' The database connector only needs to be created once in any given script.
#' chifed_db <- sqlite_connector('./db/chifed.sqlite')
#' extract_chifed_codebook(chifed_db, './MDRM.zip')
extract_chifed_codebook <- function(db_connector, codebook_zip) {
  codebook <- 
    unz(codebook_zip, 'MDRM_CSV.csv') %>% 
    read_csv(skip = 1, col_types = cols(.default = col_character())) %>%
    rename_all(function(nm) str_replace_all(toupper(nm), '\\s', '_'))
  db_conn <- db_connector()
  try(dbWriteTable(db_conn, 'CODEBOOK', codebook))
  dbDisconnect(db_conn)
}

#' What dates have already been extracted to the Chicago Database?
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @return A character vector containing dates of already-extracted datasets
#' in the Chicago database, in ISO 'YYYY-MM-DD' format
#' @importFrom dplyr distinct collect
#' @importFrom DBI dbExistsTable dbListFields dbReadTable dbDisconnect
#' @export
chifed_dates_in_db <- function(db_connector) {
  `%not_in%` <- Negate(`%in%`)
  db_conn <- db_connector()
  if (!dbExistsTable(db_conn, 'SUMMARY')) return(NULL)
  if ('REPORT_DATE' %not_in% dbListFields(db_conn, 'SUMMARY')) return(NULL)
  dates_in_db <- dbReadTable(db_conn, 'SUMMARY') %>%
    distinct(REPORT_DATE) %>% 
    collect()
  dbDisconnect(db_conn)
  return(dates_in_db)
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
#' @return NULL
#' @importFrom DBI dbWriteTable dbDisconnect
#' @importFrom dplyr mutate rename select
#' @importFrom glue glue
#' @importFrom haven read_xpt
#' @importFrom rlog log_info log_fatal
#' @importFrom tidyr pivot_longer
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' chifed_db <- sqlite_connector('./db/chifed.sqlite')
#' extract_chifed_zip(chifed_db, './zips-chifed/call0003-zip.zip')
extract_chifed_zip <- function(db_connector, zip_file) {
  `%not_in%` <- Negate(`%in%`)
  yy     <- str_match(zip_file, '([[:digit:]]{2})[[:digit:]]{2}')[1, 2]
  yyyy   <- ifelse(yy == '00', '2000', paste0('19', yy))
  mm     <- str_match(zip_file, '[[:digit:]]{2}([[:digit:]]{2})')[1, 2]
  dt_str <- glue('{yyyy}-{mm}-01')
  
  df_tbl  <- read_chifed_zip(zip_file)
  
  log_info('Getting record count per variable for `SUMMARY` table...')
  df_summ <- 
    select(df_tbl, !any_of(c('IDRSSD', 'REPORT_DATE'))) %>%
    summarize_all(~ sum(!is.na(.x) | is.null(.x))) %>%
    pivot_longer(cols      = everything(),
                 names_to  = 'VAR_NAME',
                 values_to = 'N_RECORDS') %>%
    mutate(REPORT_DATE = dt_str) %>%
    select(REPORT_DATE, VAR_NAME, 'N_RECORDS')

  # The Chicago Fed's quarterly call report data is disributed all in one table
  # each period. The table has more columns than the SQLite database backend
  # can handle without modifying and recompiling it. The quick and dirty
  # workaround is to simply pivot the data to a long form similar to what we
  # did with the data from the FFIEC. We'll rename the date and ID columns to
  # match those ones.
  
  log_info('Pivoting to long form for writing to `OBSERVATIONS` table...')
  if ('CALL8787' %not_in% df_summ$VAR_NAME) df_tbl %<>% mutate(CALL8787 = NA)
  df_out <-
    df_tbl %>%
    pivot_longer(cols = !matches(c('IDRSSD', 'REPORT_DATE',
                                   'CALL8786', 'CALL8787')),
                 names_to         = 'VAR_NAME',
                 values_to        = 'VALUE',
                 values_transform = as.character,
                 values_drop_na   = TRUE)
  
  num_vars      <- ncol(df_tbl) - 2 # Don't count `IDRSSD` or `REPORT_DATE`
  num_rows_long <- nrow(df_out)
  log_info(
    glue('Writing {num_rows_long} rows of {num_vars} variables to database...'))
  
  db_conn <- db_connector()
  tryCatch({
    dbBegin(db_conn)
    dbWriteTable(db_conn, 'OBSERVATIONS', df_out, append = TRUE)
    dbWriteTable(db_conn, 'SUMMARY', df_summ, append = TRUE)
    dbCommit(db_conn)
  }, warning = stop, error = stop)
  dbDisconnect(db_conn)
  
  log_info(glue('Successfully extracted {zip_file} to the database!'))
  cat('\n')
}

#' Extract the observations from a Chicago Fed ZIPped XPT file.
#'
#' ZIP files containing the data are available at the [Chicago Fed](https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data-complete-1976-2000).
#' They should be saved (but not extracted) to an empty folder.
#'
#' @param zip_file the path to a ZIP file on disk containing a valid XPT file
#' @return A `tibble` containing the observations in wide form.
#' @importFrom magrittr %>%
#' @importFrom glue glue
#' @importFrom haven read_xpt
#' @importFrom rlog log_info
#' @importFrom stringr str_match str_pad
#' @export
read_chifed_zip <- function(zip_file) {
  # These are distributed in SAS XPT format, an open-source format that allows
  # SAS users to distribute data in a format compatible with regulators and
  # journals requiring open-source data. They can be easily read with the
  # `read_xpt()` function in the `haven` package.
  # 
  # Some ZIP files contain superfluous files beyond the desired XPT, and the
  # XPT file uses different capitalization by period. We need to get the correct
  # name of the enclosed file before extracting it.
  yymm_str <- str_extract(zip_file, '[[:digit:]]{4}')
  xpt_file <- 
    unzip(zip_file, list = TRUE)[['Name']] %>%
    subset(str_detect(., '\\.(XPT|xpt)'))
  
  yy     <- str_match(zip_file, '([[:digit:]]{2})[[:digit:]]{2}')[1, 2]
  yyyy   <- ifelse(yy == '00', '2000', paste0('19', yy))
  mm     <- str_match(zip_file, '[[:digit:]]{2}([[:digit:]]{2})')[1, 2]
  dt_str <- glue('{yyyy}-{mm}-01')

  log_info(glue('Reading observations from {xpt_file} in {zip_file}'))
  df_tbl    <- unz(zip_file, xpt_file) %>% read_xpt()
  var_names <- names(df_tbl)
  
  log_info(glue(
    'Extracted {nrow(df_tbl)} rows containing {ncol(df_tbl)} columns'))
  
  # Most periods have the reporting institution listed as "ENTITY". For whatever
  # reason, this file has no formal "ENTITY" column and we need to copy it over
  # from another variable called "RSSD9001".
  if ('RSSD9001' %in% var_names) {
    log_info('ID Column `RSSD9001` detected...')
    df_tbl %<>% select(!any_of(c('IDRSSD', 'ENTITY')))
  } else if ('ENTITY' %in% var_names) {
    log_info('ID Column `RSSD9001` not detected. Renaming `ENTITY`-->`RSSD9001')
    df_tbl %<>% mutate(RSSD9001 = ENTITY) %>% select(!any_of(c('ENTITY')))
  } else if ('IDRSSD' %in% var_names) {
    log_info('ID Column `RSSD9001` not detected. Renaming `IDRSSD`-->`RSSD9001')
    df_tbl %<>% mutate(RSSD9001 = IDRSSD) %>% select(!any_of(c('IDRSSD')))
  } else {
    stop(glue('No valid ID column found in {xpt_file}... Cannot go on!'))
  }
  
  log_info('Renaming `RSSD9001` to `IDRSSD` for consistency with FFIEC data.')
  df_tbl %<>%
    mutate(IDRSSD = as.character(RSSD9001), REPORT_DATE = dt_str) %>%
    select(IDRSSD, REPORT_DATE, 
           !any_of(c('RSSD9999', 'RSSD9001', 'ENTITY', 'DATE')))
  log_info(glue('Done parsing {xpt_file}...'))
  return(df_tbl)
}

#' Get a data frame with all XPT files and variables for the Chicago Fed data 
#'
#' @param chifed_zip_path Folder containing the Chicago Fed zip files
#' @return A `tibble` with two columns, one with the name of the XPT file and 
#' another with the names of the variables found in that XPT file.
#' @importFrom purrr map_dfr
#' @export
get_all_chifed_varnames <- function(chifed_zip_path) {
  zips <- list_all_chifed_zips(chifed_zip_path)
  map_dfr(zips, get_chifed_varnames)
}

#' Get a data from with variables for a single Chicago Fed dataset
#'
#' @param zf A zip file containing an `XPT` file from the Chicago Fed
#'
#' @return A `tibble` with the `XPT` filename in one column and all of its
#' variables names in the other
#' @importFrom magrittr %>%
#' @importFrom glue glue
#' @importFrom haven read_xpt
#' @importFrom rlog log_info
#' @importFrom stringr str_detect
#' @export
get_chifed_varnames <- function(zf) {
  log_info(glue('Reading {zf}...'))
  xpt_file <- 
    unzip(zf, list = TRUE) %>% 
    filter(str_detect(Name, '\\.(XPT|xpt)')) %>%
    getElement('Name')
  log_info(glue('Getting variable names from {xpt_file}...'))
  df_xpt    <- unz(zf, xpt_file) %>% read_xpt(n_max = 1)
  var_names <- names(df_xpt)
  log_info(glue('Found {length(var_names)} columns in {xpt_file}'))
  cat('\n')
  return(tibble(XPT_FILE = xpt_file, VAR_NAME = names(df_xpt)))
}