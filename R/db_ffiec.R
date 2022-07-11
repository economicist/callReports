#' Query the FFIEC database for variables in a given database table
#'
#' `query_ffiec_db()` connects to a given database and searches the given
#' schedule table for the requested variables.
#'
#' @param tbl_name A valid table name containing FFIEC observations.
#' @param ... Quoted variable names, separated by commas
#' @return A `tibble` containing bank IDRSSD, report date, and the values of the
#' requested variables in wide format.
#' @export
#' @examples
#' fetch_ffiec_observations("FFIEC.OBS_RC", "RCON2170", "RCON2948", "RCON3210")
fetch_ffiec_observations <- function(tbl_name, ...) {
  db_connector <- db_connector_sqlite()
  db_conn <- db_connector()
  df_varcodes <-
    DBI::dbReadTable(db_conn, "VARCODES") %>%
    dplyr::filter(VARCODE %in% c(...)) %>%
    dplyr::collect() %>%
    tibble::as_tibble()
  varcode_ids_csv <- paste(df_varcodes$ID, collapse = ", ")
  where_clause <- glue::glue('WHERE "VARCODE_ID" IN ({varcode_ids_csv})')
  db_query <- glue::glue('SELECT * FROM "{tbl_name}" {where_clause}')

  rlog::log_info("Requesting variables from database...")
  db_res <- DBI::dbSendQuery(db_conn, db_query)
  df_out <-
    DBI::dbFetch(db_res) %>%
    tibble::as_tibble() %>%
    dplyr::mutate(REPORT_DATE = qtr_id_to_date_str(QUARTER_ID)) %>%
    dplyr::inner_join(df_varcodes, by = c("VARCODE_ID" = "ID")) %>%
    dplyr::select(IDRSSD, REPORT_DATE, VARCODE, VALUE)
  DBI::dbClearResult(db_res)
  DBI::dbDisconnect(db_conn)

  rlog::log_info(glue::glue(
    "Collected {nrow(df_out)} observations from the database."))
  rlog::log_info("Pivoting to wide format...")
  df_out %<>%
    tidyr::pivot_wider(
      id_cols = c(IDRSSD, REPORT_DATE),
      names_from = VARCODE,
      values_from = VALUE
    )

  rlog::log_info("Query completed!")
  return(df_out)
}

fetch_ffiec_observation_tables <- function() {
  db_connector <- db_connector_sqlite()
  db_conn <- db_connector()
  DBI::dbListTables(db_conn)
}

#' Quickly determine which periods are already in the FFIEC database
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @return A character vector of dates in ISO YYYY-MM-DD format
#' @export
fetch_ffiec_extracted_schedules <- function() {
  db_connector <- db_connector_sqlite()
  db_conn <- db_connector()
  if (!DBI::dbExistsTable(db_conn, "SUMMARY")) {
    DBI::dbDisconnect(db_conn)
    return(tibble::tibble())
  }
  df_out <- 
    DBI::dbReadTable(db_conn, "SUMMARY") %>%
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
#' ffiec_db <- sqlite_connector("./db/ffiec.sqlite")
#' detect_ffiec_cross_references(ffiec_db)
detect_ffiec_cross_references <- function() {
  db_connector <- db_connector_sqlite()
  db_conn <- db_connector()
  df_out <-
    DBI::dbReadTable(db_conn, "CODEBOOK") %>%
    dplyr::group_by(VARCODE) %>%
    dplyr::summarize(
      NUM_SCHEDULES = dplyr::n_distinct(SCHEDULE_CODE),
      SCHEDULE_CODE = sort(unique(SCHEDULE_CODE)),
      .groups = "drop"
    ) %>%
    dplyr::filter(NUM_SCHEDULES > 1) %>%
    dplyr::arrange(VARCODE, SCHEDULE_CODE) %>%
    dplyr::group_by(VARCODE) %>%
    dplyr::mutate(INSTANCE = row_number()) %>%
    tidyr::pivot_wider(
      id_cols = VARCODE,
      names_from = INSTANCE,
      names_glue = "SCHEDULE_{INSTANCE}",
      values_from = SCHEDULE_CODE
    ) %>%
    dplyr::arrange(SCHEDULE_1, SCHEDULE_2) %>%
    dplyr::ungroup() %>%
    dplyr::collect() %>%
    readr::type_convert()
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
#' search_ffiec_codebook(ffiec_db, var_name = "RCON2950")
#' search_ffiec_codebook(ffiec_db, var_desc = "TOTAL LIABILITIES")
search_ffiec_codebook <- 
  function(var_name = NULL, var_desc = NULL) {
    db_connector <- db_connector_sqlite()
    db_conn <- db_connector()
    df_out <- DBI::dbReadTable(db_conn, "CODEBOOK")
    if (!is.null(var_name)) {
      df_out %<>% dplyr::filter(VARCODE == toupper(var_name))
    }
    if (!is.null(var_desc)) {
      df_out %<>%
        dplyr::filter(stringr::str_detect(toupper(VAR_DESC), toupper(var_desc)))
    }
    df_out %<>%
      dplyr::filter(VARCODE == toupper(var_name)) %>%
      dplyr::distinct(SCHEDULE_CODE, VARCODE, VAR_DESC) %>%
      dplyr::collect()
    DBI::dbDisconnect(db_conn)
    return(df_out)
  }
