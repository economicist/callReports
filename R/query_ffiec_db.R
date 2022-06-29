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
#' @importFrom magrittr %>% %<>%
#' @importFrom DBI dbReadTable dbDisconnect
#' @importFrom dplyr arrange collect filter
#' @importFrom tidyr pivot_wider
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- sqlite_connector('./db/ffiec.sqlite')
#' query_ffiec_db(ffiec_db, 'RCON2170', 'RCON2948', 'RCON3210')
query_ffiec_db <- 
  function(db_connector, sch_code, ...)
  {
    var_names <- c(...)
    db_conn <- db_connector()
    df_out <-
      dbReadTable(db_conn, sch_code) %>%
      filter(VAR_NAME %in% var_names) %>%
      pivot_wider(id_cols     = c(IDRSSD, QUARTER_ID),
                  names_from  = VAR_NAME,
                  values_from = VALUE) %>%
      arrange(IDRSSD, QUARTER_ID) %>%
      mutate(REPORT_DATE = ffiec_qtr_id_to_date_str(QUARTER_ID)) %>%
      select(IDRSSD, REPORT_DATE, !any_of(c('QUARTER_ID'))) %>%
      collect()
    dbDisconnect(db_conn)
    
    df_out %<>% type_convert()
    return(df_out)
  }

#' Quickly determine which periods are already in the FFIEC database
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#'
#' @return A character vector of dates in ISO YYYY-MM-DD format
#' @importFrom magrittr %>%
#' @importFrom DBI dbExistsTable dbReadTable dbDisconnect
#' @export
ffiec_schedules_in_db <- function(db_connector) {
  db_conn <- db_connector()
  if (!dbExistsTable(db_conn, 'SUMMARY')) {
    dbDisconnect(db_conn)
    return(tibble())
  }
  df_out <- dbReadTable(db_conn, 'SUMMARY') %>% collect()
  dbDisconnect(db_conn)
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
#' @importFrom magrittr %>% %<>%
#' @importFrom DBI dbReadTable dbDisconnect
#' @importFrom dplyr collect distinct filter
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- sqlite_connector('./db/ffiec.sqlite')
#' search_ffiec_codebook(ffiec_db, var_name = 'RCON2950')
#' search_ffiec_codebook(ffiec_db, var_desc = 'TOTAL LIABILITIES')
search_ffiec_codebook <- function(db_connector, var_name = NULL, var_desc = NULL) {
  db_conn <- db_connector()
  df_out <- dbReadTable(db_conn, 'CODEBOOK')
  if (!is.null(var_name)) {
    df_out %<>% filter(VAR_NAME == toupper(var_name))
  }
  if (!is.null(var_desc)) {
    df_out %<>% 
      filter(str_detect(toupper(VAR_DESC), toupper(var_desc)))
  }
  df_out %<>%
    filter(VAR_NAME == toupper(var_name)) %>%
    distinct(SCHEDULE_CODE, VAR_NAME, VAR_DESC) %>%
    collect()
  dbDisconnect(db_conn)
  return(df_out)
}

#' Detect variables in the FFIEC database that exist in multiple tables
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @return A `tibble` telling you which tables a variable appearing in multiple
#' parts of the FFIEC reporting form can be found
#' @importFrom magrittr %>%
#' @importFrom DBI dbReadTable dbDisconnect
#' @importFrom dplyr arrange collect filter group_by mutate summarize ungroup
#' @importFrom readr type_convert
#' @importFrom tidyr pivot_wider
#' @export
#' @examples
#' The database connector only needs to be created once in any given script.
#' ffiec_db <- sqlite_connector('./db/ffiec.sqlite')
#' detect_ffiec_cross_references(ffiec_db)
detect_ffiec_cross_references <- function(db_connector) {
  db_conn <- db_connector()
  df_out <-
    dbReadTable(db_conn, 'CODEBOOK') %>%
    group_by(VAR_NAME) %>%
    summarize(NUM_SCHEDULES = n_distinct(SCHEDULE_CODE),
                     SCHEDULE_CODE = sort(unique(SCHEDULE_CODE)),
                     .groups = 'drop') %>%
    filter(NUM_SCHEDULES > 1) %>%
    arrange(VAR_NAME, SCHEDULE_CODE) %>%
    group_by(VAR_NAME) %>%
    mutate(INSTANCE = row_number()) %>%
    pivot_wider(id_cols = VAR_NAME,
                       names_from = INSTANCE,
                       names_glue = 'SCHEDULE_{INSTANCE}',
                       values_from = SCHEDULE_CODE) %>%
    arrange(SCHEDULE_1, SCHEDULE_2) %>%
    ungroup() %>%
    collect() %>%
    type_convert()
  dbDisconnect(db_conn)
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
#' @importFrom magrittr %>%
#' @importFrom DBI dbReadTable dbDisconnect
#' @importFrom dplyr distinct filter
#' @importFrom glue glue
#' @importFrom purrr map_dfr
#' @importFrom rlog log_info
#' @importFrom readr type_convert
#' @importFrom tidyr pivot_wider
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- sqlite_connector('./db/ffiec.sqlite')
#' query_multischedule_variable(ffiec_db, 'RCON2170')
query_multischedule_variable <- function(db_connector, var_name) {
  db_conn <- db_connector()
  
  log_info(
    glue('Checking codebook to find schedules containing {var_name}...')
  )
  schedules <- 
    dbReadTable(db_conn, 'CODEBOOK') %>%
    filter(VAR_NAME == str_to_upper(var_name)) %>%
    distinct(SCHEDULE_CODE) %>%
    .$SCHEDULE_CODE
  
  if (length(schedules) == 1) {
    log_info(glue('Found only in schedule {sch}...'))
    log_info(glue('Querying {var_name} from {sch}...'))
    cat('\n')
    return(get_var_from_schedule(schedules[1], var_name))
  }
  
  schedules_str <- paste(schedules, collapse = ', ')
  log_info(
    glue('Variable {var_name} found in schedules {schedules_str}...')
  )
  df_out <-
    map_dfr(schedules, function(sch) {
      log_info(glue('Querying {var_name} from {sch}...'))
      return(get_var_from_schedule(sch, var_name))
    })
  
  dbDisconnect(db_conn)
  cat('\n')
  
  df_out %>%
    pivot_wider(id_cols = c(IDRSSD, REPORT_DATE),
                       names_from  = SCHEDULE_CODE,
                       values_from = VALUE) %>%
    type_convert()
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
#' @importFrom magrittr %>%
#' @importFrom DBI dbReadTable dbDisconnect
#' @importFrom dplyr distinct collect
#' @export
zipped_schedules_not_in_db <- function(db_connector, zip_file) {
  schedules_in_zip <- 
    list_ffiec_schedules(zip_file) %>%
    map_dfr(function(sch) {
      tibble(REPORT_DATE   = extract_ffiec_datestr(sch),
             SCHEDULE_CODE = extract_schedule_code(sch),
             PART_NUM      = extract_part_codes(sch)[1],
             PART_OF       = extract_part_codes(sch)[2])
    })
  sch_date <- unique(schedules_in_zip$REPORT_DATE)
  
  db_conn <- db_connector()
  df_summ <- 
    dbReadTable(db_conn, 'SUMMARY') %>%
    filter(REPORT_DATE == sch_date) %>%
    distinct(REPORT_DATE, SCHEDULE_CODE, PART_NUM, PART_OF) %>%
    collect() %>% as_tibble() %>%
    mutate_at(vars(starts_with('PART_')), ~ as.numeric(.x))
  dbDisconnect(db_conn)
  
  schedules_not_in_db <-
    schedules_in_zip %>%
    anti_join(df_summ,
              by = c('REPORT_DATE', 'SCHEDULE_CODE', 'PART_NUM', 'PART_OF')) %>%
    pmap_chr(callReports::build_schedule_filename)
  
  return(schedules_not_in_db)
}