#' Write a dataframe of Chicago Fed observations to the database
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param df_long A dataframe of observations in long form
#' @export
write_chifed_observations <- function(df_long) {
  db_connector <- db_connector_sqlite()
  db_conn <- db_connector()
  tryCatch(
    {
      DBI::dbBegin(db_conn)
      if (!DBI::dbExistsTable(db_conn, "CHIFED.OBS_ALL")) {
        DBI::dbCreateTable(db_conn, "CHIFED.OBS_ALL",
          fields = c(
            IDRSSD = "INTEGER",
            QUARTER_ID = "INTEGER",
            CALL8786 = "INTEGER",
            CALL8787 = "INTEGER",
            VARCODE_ID = "INTEGER",
            VALUE = "TEXT"
          )
        )
      }
      DBI::dbWriteTable(db_conn, "CHIFED.OBS_ALL", df_long, append = TRUE)
      DBI::dbCommit(db_conn)
    },
    warning = stop,
    error = stop
  )
  DBI::dbDisconnect(db_conn)
}

#' Retreive Chicago Fed observations from a database
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param ... Quoted variable names, separated by commas.
#' @return A `tibble()` containing bank ID, report date, and values for the
#' variables requested in `...`
#' @export
#' @examples
#' chifed_db <- sqlite_connector("./db/chifed.sqlite")
#' query_chifed_db("RCON2170", "RCON2950", "RCON3210")
fetch_chifed_observations <-
  function(tbl_name, ..., min_date = NULL, max_date = NULL) {
    db_connector <- db_connector_sqlite()
    db_conn <- db_connector()
    df_varcodes <-
      DBI::dbReadTable(db_conn, "VARCODES") %>%
      dplyr::filter(VARCODE %in% c(...)) %>%
      dplyr::collect() %>%
      tibble::as_tibble()
    varcode_ids_csv <- paste(df_varcodes$ID, collapse = ", ")
    where_clause <- glue::glue('WHERE "VARCODE_ID" IN ({varcode_ids_csv})')
    if (!is.null(min_date)) {
      min_qtr_id <- ymd_chr_to_qtr_id(min_date)
      where_clause <- glue::glue("{where_clause} AND QUARTER_ID >= {min_qtr_id}")
    }
    if (!is.null(max_date)) {
      max_qtr_id <- ymd_chr_to_qtr_id(max_date)
      where_clause <- glue::glue("{where_clause} AND QUARTER_ID <= {max_qtr_id}")
    }
    db_query <- glue::glue('SELECT * FROM "CHIFED.OBS_ALL" {where_clause}')

    rlog::log_info("Requesting variables from database...")
    db_res <- DBI::dbSendQuery(db_conn, db_query)
    df_out <-
      DBI::dbFetch(db_res) %>%
      tibble::as_tibble() %>%
      dplyr::mutate(REPORT_DATE = qtr_id_to_ymd_chr(QUARTER_ID)) %>%
      dplyr::inner_join(df_varcodes, by = c("VARCODE_ID" = "ID")) %>%
      dplyr::select(IDRSSD, REPORT_DATE, CALL8786, CALL8787, VARCODE, VALUE)
    DBI::dbClearResult(db_res)
    DBI::dbDisconnect(db_conn)

    rlog::log_info(glue::glue(
      "Collected {nrow(df_out)} observations from the database."
    ))
    rlog::log_info("Pivoting to wide format...")
    df_out %<>%
      tidyr::pivot_wider(
        id_cols = c(IDRSSD, REPORT_DATE, CALL8786, CALL8787),
        names_from = VARCODE,
        values_from = VALUE
      )

    rlog::log_info("Query completed!")
    return(df_out)
  }

#' Fetch the Chicago Fed codebook data for a set of database variable ID codes.
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param varcode_ids A vector of integer-valued variable code IDs that can
#' be found in the `VARCODES` database table.
#' @export
fetch_chifed_codebook_by_varcode_ids <- function(db_connector, varcode_ids) {
  db_conn <- db_connector()
  db_res <-
    paste(varcode_ids, collapse = ", ") %>%
    paste(
      'SELECT A."ID" AS "VARCODE_ID", B.* FROM "VARCODES" A',
      'INNER JOIN "CHIFED.CODEBOOK" B',
      'ON A."VARCODE" = B."VARCODE"',
      'WHERE A."ID" IN (', ., ")"
    ) %>%
    DBI::dbSendQuery(db_conn, .)
  df_out <-
    DBI::dbFetch(db_res) %>%
    as_tibble() %>%
    select(!any_of(c("MNEMONIC", "ITEM_CODE"))) %>%
    mutate(across(START_DATE:END_DATE, ~ as.Date(lubridate::mdy_hms(.)))) %>%
    mutate(DESCRIPTION = str_remove_all(DESCRIPTION, "^[\\n\\r\\s]+"))
  DBI::dbClearResult(db_res)
  DBI::dbDisconnect(db_conn)
  return(df_out)
}

#' Which periods of data have already been extracted to the Chicago Database?
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @return A character vector containing dates of already-extracted datasets
#' in the Chicago database, in ISO 'YYYY-MM-DD' format
#' @export
available_chifed_dates <- function(db_connector) {
  `%not_in%` <- Negate(`%in%`)
  db_conn <- db_connector()
  if (!DBI::dbExistsTable(db_conn, "SUMMARY")) {
    return(NULL)
  }
  if ("REPORT_DATE" %not_in% DBI::dbListFields(db_conn, "SUMMARY")) {
    return(NULL)
  }
  dates_in_db <-
    DBI::dbReadTable(db_conn, "SUMMARY") %>%
    dplyr::distinct(REPORT_DATE) %>%
    dplyr::collect()
  DBI::dbDisconnect(db_conn)
  return(dates_in_db)
}

#' Get information about variables currently available in the extracted Chicago
#' database
#'
#' `available_chifed_varcodes()` returns a subset of the codebook table found
#' in the database table `CHIFED.CODEBOOK` containing records only for those
#' variables currently available in `CHIFED.OBS_ALL` (as determined by a query
#' at the time of this function being called), as well as an additional column
#' indicating which value of `VARCODE_ID` in `CHIFED.OBS_ALL` is associated
#' with which `VARCODE` and description.
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @return A `tibble` in the form of the codebook, but with an additional
#' column pairing a `VARCODE_ID` to each `VARCODE` found in the observations
#' table
#' @export
available_chifed_varcodes <-
  function(db_connector, min_date = NULL, max_date = NULL) {
    db_query <- 'SELECT DISTINCT "VARCODE_ID" FROM "CHIFED.OBS_ALL"'

    where_predicates <- c()
    if (!is.null(min_date)) {
      min_qtr_id <- ymd_chr_to_qtr_id(min_date)
      where_predicates %<>% c('"QUARTER_ID" >= {min_qtr_id}')
    }
    if (!is.null(max_date)) {
      max_qtr_id <- ymd_chr_to_qtr_id(max_date)
      where_predicates %<>% c('"QUARTER_ID" <= {max_qtr_id}')
    }
    where_pred_and <- paste(where_predicates, collapse = " AND ")
    where_clause <- ifelse(length(where_predicates) == 0, "",
      paste("WHERE", where_pred_and)
    )

    db_conn <- db_connector()
    rlog::log_info("Requesting available variables from the database.")
    db_results <- DBI::dbSendQuery(db_conn, db_query)
    df_return <- DBI::dbFetch(db_results) %>% as_tibble()
    num_vars <- nrow(df_return)
    rlog::log_info(glue::glue("Retrieved {num_vars} distinct variable IDs."))
    DBI::dbClearResult(db_results)

    df_varcode_ids_all <-
      DBI::dbReadTable(db_conn, "VARCODES") %>%
      collect() %>%
      as_tibble()

    df_codebook <-
      DBI::dbReadTable(db_conn, "CHIFED.CODEBOOK") %>%
      collect() %>%
      as_tibble()

    df_details <-
      df_return %>%
      inner_join(df_varcode_ids_all, by = c("VARCODE_ID" = "ID")) %>%
      mutate(
        MNEMONIC = substr(VARCODE, 1, 4),
        ITEM_CODE = substr(VARCODE, 5, 8)
      ) %>%
      inner_join(df_codebook, by = c("MNEMONIC", "ITEM_CODE")) %>%
      mutate(across(START_DATE:END_DATE, ~ as.Date(lubridate::mdy_hms(.)))) %>%
      filter(CONFIDENTIALITY == "N")

    if (!is.null(max_date)) {
      df_details %<>% filter(START_DATE <= lubridate::ymd(max_date))
    }
    if (!is.null(min_date)) {
      df_details %<>% filter(END_DATE >= lubridate::ymd(min_date))
    }

    df_details %<>%
      select(
        VARCODE_ID, VARCODE, REPORTING_FORM,
        !any_of(c(
          "MNEMONIC", "ITEM_CODE",
          "CONFIDENTIALITY", "SERIESGLOSSARY"
        ))
      ) %>%
      arrange(VARCODE, REPORTING_FORM) %>%
      as_tibble()

    DBI::dbDisconnect(db_conn)
    return(df_details)
  }
