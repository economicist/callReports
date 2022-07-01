#' Query the FFIEC database for variables in a given database table
#' 
#' `query_ffiec_db()` connects to a given database and searches the given
#' schedule table for the requested variables.
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @param sch_code A valid alphabetical schedule code
#' @param ... Quoted variable names, separated by commas
#' @return A `tibble` containing bank ID, report date, and the values of the
#' requested variables.
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- sqlite_connector('./db/ffiec.sqlite')
#' query_ffiec_db(ffiec_db, 'RCON2170', 'RCON2948', 'RCON3210')
query_ffiec_db <- function(db_connector, sch_code, ...) {
  var_codes <- c(...)
  db_conn <- db_connector()
  tbl_varcodes <-
    DBI::dbReadTable(db_conn, 'VAR_CODES') %>%
    dplyr::filter(VAR_CODE %in% var_codes)
  df_out <-
    DBI::dbReadTable(db_conn, sch_code) %>%
    dplyr::inner_join(tbl_varcodes, by = c('VAR_CODE_ID' = 'ID')) %>%
    dplyr::mutate(REPORT_DATE = qtr_id_to_date_str(QUARTER_ID)) %>%
    dplyr::select(IDRSSD, REPORT_DATE, VAR_CODE, VALUE) %>%
    tidyr::pivot_wider(id_cols     = c(IDRSSD, REPORT_DATE),
                names_from  = VAR_CODE,
                values_from = VALUE) %>%
    dplyr::arrange(IDRSSD, REPORT_DATE) %>%
    dplyr::collect() %>%
    readr::type_convert()
  DBI::dbDisconnect(db_conn)
  return(df_out)
}

#' Quickly determine which periods are already in the FFIEC database
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @return A character vector of dates in ISO YYYY-MM-DD format
#' @export
ffiec_schedules_in_db <- function(db_connector) {
  db_conn <- db_connector()
  if (!DBI::dbExistsTable(db_conn, 'SUMMARY')) {
    DBI::dbDisconnect(db_conn)
    return(tibble::tibble())
  }
  df_out <- DBI::dbReadTable(db_conn, 'SUMMARY') %>% dplyr::collect()
  DBI::dbDisconnect(db_conn)
  return(df_out)
}

#' Search the FFIEC data codebook in the database, by varible name or description
#'
#' Either a `var_name` or `var_desc` must be provided.
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @param var_name A valid full 8-character variable name
#' @param var_desc Any substring (case-insensitive) you hope to find in a
#' variable description
#' @return A `tibble` containing matching results in the codebook
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- sqlite_connector('./db/ffiec.sqlite')
#' search_ffiec_codebook(ffiec_db, var_name = 'RCON2950')
#' search_ffiec_codebook(ffiec_db, var_desc = 'TOTAL LIABILITIES')
search_ffiec_codebook <- function(db_connector, var_name = NULL, var_desc = NULL) {
  db_conn <- db_connector()
  df_out <- DBI::dbReadTable(db_conn, 'CODEBOOK')
  if (!is.null(var_name)) {
    df_out %<>% dplyr::filter(VAR_CODE == toupper(var_name))
  }
  if (!is.null(var_desc)) {
    df_out %<>% 
      dplyr::filter(stringr::str_detect(toupper(VAR_DESC), toupper(var_desc)))
  }
  df_out %<>%
    dplyr::filter(VAR_CODE == toupper(var_name)) %>%
    dplyr::distinct(SCHEDULE_CODE, VAR_CODE, VAR_DESC) %>%
    dplyr::collect()
  DBI::dbDisconnect(db_conn)
  return(df_out)
}

#' Detect variables in the FFIEC database that exist in multiple tables
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @return A `tibble` telling you which tables a variable appearing in multiple
#' parts of the FFIEC reporting form can be found
#' @export
#' @examples
#' The database connector only needs to be created once in any given script.
#' ffiec_db <- sqlite_connector('./db/ffiec.sqlite')
#' detect_ffiec_cross_references(ffiec_db)
detect_ffiec_cross_references <- function(db_connector) {
  db_conn <- db_connector()
  df_out <-
    DBI::dbReadTable(db_conn, 'CODEBOOK') %>%
    dplyr::group_by(VAR_CODE) %>%
    dplyr::summarize(NUM_SCHEDULES = dplyr::n_distinct(SCHEDULE_CODE),
              SCHEDULE_CODE = sort(unique(SCHEDULE_CODE)),
              .groups = 'drop') %>%
    dplyr::filter(NUM_SCHEDULES > 1) %>%
    dplyr::arrange(VAR_CODE, SCHEDULE_CODE) %>%
    dplyr::group_by(VAR_CODE) %>%
    dplyr::mutate(INSTANCE = row_number()) %>%
    tidyr::pivot_wider(id_cols = VAR_CODE,
                names_from = INSTANCE,
                names_glue = 'SCHEDULE_{INSTANCE}',
                values_from = SCHEDULE_CODE) %>%
    dplyr::arrange(SCHEDULE_1, SCHEDULE_2) %>%
    dplyr::ungroup() %>%
    dplyr::collect() %>%
    readr::type_convert()
  DBI::dbDisconnect(db_conn)
  return(df_out)
}

#' Query a variable from all FFIEC tables where it exists
#' 
#' In case you want to know which FFIEC schedule contains the most instances
#' of a given variable, you can use this function to query all the tables where
#' it exists. You can view the results to determine which schedule you should
#' query for that variable in the future.
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @param var_name A variable that exists in multiple schedules, as determined
#' by the results of `search_ffiec_codebook()`
#' @return A `tibble` in the same format as results of `query_ffiec_db()`, but
#' the column names represent the schedule in which the column is from
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- sqlite_connector('./db/ffiec.sqlite')
#' query_multischedule_variable(ffiec_db, 'RCON2170')
query_multischedule_variable <- function(db_connector, var_name) {
  db_conn <- db_connector()
  
  rlog::log_info(
    glue::glue('Checking codebook to find schedules containing {var_name}...')
  )
  schedules <- 
    DBI::dbReadTable(db_conn, 'CODEBOOK') %>%
    dplyr::filter(VAR_CODE == str_to_upper(var_name)) %>%
    dplyr::distinct(SCHEDULE_CODE) %>%
    .$SCHEDULE_CODE
  
  if (length(schedules) == 1) {
    rlog::log_info(glue::glue('Found only in schedule {sch}...'))
    rlog::log_info(glue::glue('Querying {var_name} from {sch}...'))
    cat('\n')
    return(get_var_from_schedule(schedules[1], var_name))
  }
  
  schedules_str <- paste(schedules, collapse = ', ')
  rlog::log_info(
    glue::glue('Variable {var_name} found in schedules {schedules_str}...')
  )
  df_out <-
    purrr::map_dfr(schedules, function(sch) {
      rlog::log_info(glue::glue('Querying {var_name} from {sch}...'))
      return(get_var_from_schedule(sch, var_name))
    })
  
  DBI::dbDisconnect(db_conn)
  cat('\n')
  
  df_out %>%
    tidyr::pivot_wider(id_cols = c(IDRSSD, REPORT_DATE),
                       names_from  = SCHEDULE_CODE,
                       values_from = VALUE) %>%
    readr::type_convert()
}

#' How much of this ZIP file has been extracted to the database?
#' 
#' `compare_zip_to_db()` queries the `SUMMARY` table of the database, given by
#' the connector function `db_connector()`, to determine which schedules for
#' a particular period's FFIEC ZIP file have already been extracted to that
#' database, so as to allow `extract_all_ffiec_zips()` to begin where it left
#' off in its previous run.
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @return A `tibble` containing a record for each period, schedule, and part
#' code already extracted to the database, as determined by a query of the 
#' `SUMMARY` table.
#' @export
zipped_schedules_not_in_db <- function(db_connector, zip_file) {
  schedules_in_zip <- 
    list_ffiec_schedules(zip_file) %>%
    purrr::map_dfr(function(sch) {
      tibble::tibble(REPORT_DATE   = extract_ffiec_datestr(sch),
                     SCHEDULE_CODE = extract_schedule_code(sch),
                     PART_NUM      = extract_part_codes(sch)[1],
                     PART_OF       = extract_part_codes(sch)[2])
    })
  sch_date <- unique(schedules_in_zip$REPORT_DATE)
  
  db_conn <- db_connector()
  df_summ <- 
    DBI::dbReadTable(db_conn, 'SUMMARY') %>%
    dplyr::filter(REPORT_DATE == sch_date) %>%
    dplyr::distinct(REPORT_DATE, SCHEDULE_CODE, PART_NUM, PART_OF) %>%
    dplyr::collect() %>% tibble::as_tibble() %>%
    mutate_at(vars(starts_with('PART_')), ~ as.numeric(.x))
  DBI::dbDisconnect(db_conn)
  
  schedules_not_in_db <-
    schedules_in_zip %>%
    anti_join(df_summ,
              by = c('REPORT_DATE', 'SCHEDULE_CODE', 'PART_NUM', 'PART_OF')) %>%
    purrr::map_chr(build_schedule_filename)
  
  return(schedules_not_in_db)
}
