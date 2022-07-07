#' List ZIP files containing Call Report data from the Chicago Fed
#' 
#' `list_chifed_zips()` expects the path of a folder containing the bulk data
#' files for 1976-2000 offered at the [Chicago Fed's website](https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data-complete-1976-2000) 
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @param chifed_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' SQLite DB? This is meaningful only for SQLite databases.
#' @return A character vector containing the full paths of valid ZIP files
#' @export
#' @examples
#' extract_chifed_zips('./zips-chifed')
extract_chifed_zips <- 
  function(db_connector, chifed_zip_path = get_chifed_zip_dir()) {
    closeAllConnections()
    codebook_path <- glue::glue('{chifed_zip_path}/MDRM.zip')
    capture.output({
      extract_chifed_mdrm(db_connector, codebook_path)
      list_chifed_zips(chifed_zip_path) %>%
        purrr::walk(~ extract_chifed_zip(db_connector, .))
    },
    file = generate_log_name('extraction_chifed'),
    split = TRUE)
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
extract_chifed_zip <- function(db_connector, zip_file = NULL) {
  if (is.null(zip_file)) {
    zip_file <- rstudioapi::selectFile(path = get_chifed_zip_dir())
  }
  yy     <- stringr::str_match(zip_file, '([[:digit:]]{2})[[:digit:]]{2}')[1, 2]
  yyyy   <- ifelse(yy == '00', '2000', paste0('19', yy))
  mm     <- stringr::str_match(zip_file, '[[:digit:]]{2}([[:digit:]]{2})')[1, 2]
  dt_str <- glue::glue('{yyyy}-{mm}-01')

  xpt_file <- 
    unzip(zip_file, list = TRUE)[['Name']] %>%
    subset(stringr::str_detect(., '\\.(XPT|xpt)'))
  
  zip_base <- basename(zip_file)
  rlog::log_info(glue::glue('Reading records from {zip_base} > {xpt_file}'))
  df_wide <- unz(zip_file, xpt_file) %>% haven::read_xpt()
  rlog::log_info(glue::glue('Extracted {nrow(df_wide)} rows ', 
                            'with {ncol(df_wide)} columns from {xpt_file}'))
  
  df_wide %<>%
    dplyr::mutate(IDRSSD = as.character(RSSD9001),
                  QUARTER_ID = date_str_to_qtr_id(dt_str)) %>%
    dplyr::select(IDRSSD, QUARTER_ID, 
                  !any_of(c('RSSD9999', 'RSSD9001', 'ENTITY', 'DATE')))
  
  xpt_varcodes <- names(df_wide)
  new_varcodes <- xpt_varcodes %>% subset(. %not_in% c('IDRSSD', 'QUARTER_ID'))
  write_varcodes(db_connector, new_varcodes)

  rlog::log_info(glue::glue(
    'Pivoting to long form for `CHIFED.OBS_ALL` table...'))
  if ('CALL8787' %not_in% new_varcodes) {
    df_wide %<>% dplyr::mutate(CALL8787 = NA)
  }
  
  df_long <-
    df_wide %>%
    tidyr::pivot_longer(
      cols = !any_of(c('IDRSSD', 'QUARTER_ID', 'CALL8786', 'CALL8787')),
      names_to         = 'VAR_CODE',
      values_to        = 'VALUE',
      values_transform = as.character,
      values_drop_na   = TRUE
    ) %>%
    dplyr::inner_join(fetch_varcodes(db_connector), by = 'VAR_CODE') %>%
    dplyr::rename(VAR_CODE_ID = ID) %>%
    dplyr::select(IDRSSD, QUARTER_ID, CALL8786, CALL8787, VAR_CODE_ID, VALUE)
  
  num_vars <- ncol(df_wide) - 2 # Don't count `IDRSSD` or `QUARTER_ID`
  num_obs  <- nrow(df_long)
  rlog::log_info(glue::glue('Writing {num_obs} non-NA observations',
                            ' of {num_vars} variables to database...'))
  tryCatch(write_chifed_observations(db_connector, df_long),
           warning = stop,
           error   = stop)
  rlog::log_info(glue::glue(
    'Successfully extracted {zip_base} > {xpt_file} to the database!'))
  cat('\n')
}

#' Extract the Chicago Fed's codebook data to a database
#'
#' `extract_chifed_mdrm()` extracts the Chicago Fed's call report data
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
#' extract_chifed_mdrm(chifed_db, './MDRM.zip')
extract_chifed_mdrm <- function(db_connector, codebook_zip) {
  mdrm <- 
    unz(codebook_zip, 'MDRM_CSV.csv') %>% 
    readr::read_csv(skip        = 1, 
                    col_types   = readr::cols(.default = 'c'),
                    name_repair = repair_colnames,
                    progress    = FALSE) %>%
    dplyr::select(-SERIESGLOSSARY, -UNNAMED_11) %>%
    dplyr::mutate(VAR_CODE = paste0(MNEMONIC, ITEM_CODE)) %>%
    dplyr::select(VAR_CODE, everything())
  db_conn <- db_connector()
  try(DBI::dbWriteTable(db_conn, 'CHIFED.CODEBOOK', mdrm, overwrite = TRUE))
  DBI::dbDisconnect(db_conn)
}


