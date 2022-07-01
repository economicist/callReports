#' Retreive Chicago Fed observations from a database
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @param ... Quoted variable names, separated by commas.
#' @return A `tibble()` containing bank ID, report date, and values for the
#' variables requested in `...`
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' chifed_db <- sqlite_connector('./db/chifed.sqlite')
#' query_chifed_db('RCON2170', 'RCON2950', 'RCON3210')
query_chifed_db <- function(db_connector, tbl_name, ...) {
  db_conn <- db_connector()
  df_varcodes <-
    DBI::dbReadTable(db_conn, 'VAR_CODES') %>%
    dplyr::filter(VAR_CODE %in% c(...)) %>%
    dplyr::collect() %>%
    tibble::as_tibble()
  varcode_ids_csv <- paste(df_varcodes$ID, collapse = ', ')
  where_clause    <- glue::glue('WHERE "VAR_CODE_ID" IN ({varcode_ids_csv})')
  db_query        <- glue::glue('SELECT * FROM "CHIFED.OBS_ALL" {where_clause}')

  rlog::log_info('Requesting variables from database...')
  db_res <- DBI::dbSendQuery(db_conn, db_query)
  df_out <- 
    DBI::dbFetch(db_res) %>% tibble::as_tibble() %>%
    dplyr::mutate(REPORT_DATE = qtr_id_to_date_str(QUARTER_ID)) %>%
    dplyr::inner_join(df_varcodes, by = c('VAR_CODE_ID' = 'ID')) %>%
    dplyr::select(IDRSSD, REPORT_DATE, CALL8786, CALL8787, VAR_CODE, VALUE)
  DBI::dbClearResult(db_res)
  DBI::dbDisconnect(db_conn)

  rlog::log_info(glue::glue('Collected {nrow(df_out)} observations from the database.'))
  rlog::log_info('Pivoting to wide format...')
  df_out %<>%
    tidyr::pivot_wider(id_cols     = c(IDRSSD, REPORT_DATE, CALL8786, CALL8787),
                names_from  = VAR_CODE,
                values_from = VALUE)
  
  rlog::log_info('Query completed!')
  return(df_out)  
}

#' Search the Chicago Fed data codebook in the database, by name or description
#'
#' Either a `var_name` or `var_desc` must be provided.
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @param var_name Either a four-letter variable prefix or full 8-character name
#' @param var_desc Any substring (case-insensitive) you hope to find in a
#' variable description
#' @return A `tibble` containing matching results in the codebook
#' @export
#' @examples
#' The database connector only needs to be created once in any given script.
#' chifed_db <- sqlite_connector('./db/chifed.sqlite')
#' search_chifed_codebook(var_name = 'RCON')
#' search_chifed_codebook(var_name = 'RCON2950')
#' search_chifed_codebook(var_desc = 'TOTAL LIABILITIES')
search_chifed_codebook <- function(db_connector, var_name = NULL, var_desc = NULL) {
  if (is.null(var_name) & is.null(var_desc)) return(NULL)
  mnemonic <- ifelse(!is.null(var_name), substr(var_name, 1, 4), NULL)
  itemcode <- ifelse(!is.null(var_name) & stringr::str_length(var_name) == 8,
                     substr(var_name, 5, 8),
                     NULL)
  var_desc <- ifelse(!is.null(var_desc), toupper(var_desc), NULL)
  where_predicates <-
    paste(c(
      ifelse(!is.null(mnemonic), glue::glue('"Mnemonic" == "{mnemonic}"'), NULL),
      ifelse(!is.null(itemcode), glue::glue('"Item Code" == "{itemcode}"'), NULL),
      ifelse(!is.null(var_desc),
             glue::glue('UPPER("Item Name") LIKE %{var_desc}%'),
             NULL)
    ), collapse = ' AND ')
  
  db_conn <- db_connector()
  query <- glue::glue('SELECT * FROM CODEBOOK WHERE Mnemonic == "{where_clause}"')
  db_results <- DBI::dbSendQuery(db_conn, query)
  df_out <- DBI::dbFetch(db_results)
  DBI::dbClearResult(db_results)
  DBI::dbDisconnect(db_conn)
  return(df_out)
}
