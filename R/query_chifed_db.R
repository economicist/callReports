#' Retreive Chicago Fed observations from a database
#'
#' @param db_connector A `function` created by one of the `db_connector_*()` 
#' functions found in this package. It should be passed without the `()`
#' @param ... Quoted variable names, separated by commas.
#' @return A `tibble()` containing bank ID, report date, and values for the
#' variables requested in `...`
#' @importFrom magrittr %>%
#' @importFrom DBI dbSendQuery dbClearResult dbDisconnect
#' @importFrom glue glue
#' @importFrom rlog log_info
#' @importFrom tibble as_tibble
#' @importFrom tidyr pivot_wider
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' chifed_db <- sqlite_connector('./db/chifed.sqlite')
#' query_chifed_db('RCON2170', 'RCON2950', 'RCON3210')
query_chifed_db <- function(db_connector, ...) {
  # Using `dbReadTable()` with this many observations appears to cause crashes
  # when working with SQLite databases. Using `dbSendQuery()` seems to avoid
  # such problems.
  var_names <- c(...)
  where_clause <- 
    sapply(var_names, function(var_name) {
      glue('VAR_NAME == "{var_name}"')
    }) %>% paste(collapse = ' OR ')
  
  log_info('Requesting variables from database...')
  query <- glue('SELECT * FROM OBSERVATIONS WHERE {where_clause}')
  db_conn <- db_connector()
  db_res  <- dbSendQuery(db_conn, query)
  df_out  <- as_tibble(dbFetch(db_res))
  dbClearResult(db_res)
  dbDisconnect(db_conn)

  log_info('Pivoting to wide format...')
  df_out %<>% 
    pivot_wider(id_cols     = c(IDRSSD, REPORT_DATE, CALL8786, CALL8787),
                names_from  = VAR_NAME,
                values_from = VALUE)
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
#' @importFrom magrittr %>%
#' @importFrom DBI dbSendQuery dbFetch dbClearResult dbDisconnect
#' @importFrom stringr str_length
#' @importFrom tibble as_tibble
#' @export
#' @examples
#' The database connector only needs to be created once in any given script.
#' chifed_db <- sqlite_connector('./db/chifed.sqlite')
#' search_chifed_codebook(var_name = 'RCON')
#' search_chifed_codebook(var_name = 'RCON2950')
#' search_chifed_codebook(var_desc = 'TOTAL LIABILITIES')
search_chifed_codebook <- function(db_connector, var_name = NULL, var_desc = NULL) {
  if (is.null(var_name) & is.null(var_desc)) {
    return(NULL)
  }
  
  mnemonic <- ifelse(!is.null(var_name), substr(var_name, 1, 4), NULL)
  itemcode <- ifelse(!is.null(var_name) & str_length(var_name) == 8,
                     substr(var_name, 5, 8),
                     NULL)
  var_desc <- ifelse(!is.null(var_desc), toupper(var_desc), NULL)
  where_predicates <-
    paste(c(
      ifelse(!is.null(mnemonic), glue('"Mnemonic" == "{mnemonic}"'), NULL),
      ifelse(!is.null(itemcode), glue('"Item Code" == "{itemcode}"'), NULL),
      ifelse(!is.null(var_desc),
             glue('UPPER("Item Name") LIKE %{var_desc}%'),
             NULL)
    ), collapse = ' AND ')
  
  db_conn <- db_connector()
  query <- glue('SELECT * FROM CODEBOOK WHERE Mnemonic == "{where_clause}"')
  db_results <- dbSendQuery(db_conn, query)
  df_out <- dbFetch(db_results)
  dbClearResult(db_results)
  dbDisconnect(db_conn)
  return(df_out)
}