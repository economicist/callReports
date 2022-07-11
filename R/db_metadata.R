#' Add variable names to an index table
#'
#' `write_varcodes()` takes a list of variable codes and adds each of them
#' to a database table containing only a pairing between an integer ID and the
#' variable code. If the variable code is already in the table, it is skipped.
#' If it is not, it is automatically assigned an integer ID value in the order
#' in which it is added.
#'
#' Given the numerous variables in the source data, this library pivots the data
#' into long form before writing to the database. This offers the benefit of
#' allowing each table in the database to have a predictable description and
#' prevent issues with exceeding the native maximum column support of many
#' database engines.
#'
#' However, pivoting the data creates a text column with a copy of the variable
#' code being stored for every one of the millions of observations in the data
#' set, significantly expanding the storage requirement of the database, and
#' potentially making queries run noticeably slower on standard spinning-platter
#' hard drives due to the increased scanning time.
#'
#' This function aims to alleviate that problem by assigning each variable code
#' an integer ID value so that the storage requirements are significantly
#' smaller.
#'
#' @param varcodes A character vector of variable codes to put into the
#' database. Variable codes already assigned a database ID will be skipped.
#' @param db_conn An active database connection created by `db_connector_*()`
#' @export
write_varcodes <- function(varcodes, db_conn = NULL) {
  if (is.null(db_conn)) {
    db_connector <- db_connector_sqlite()
    db_conn <- db_connector()
  }

  # If the `VARCODES` table does not yet exist, create it with an ID column
  # that automatically increments with each new addition to the table, thus
  # automatically assigning each variable code an integer ID. Additionally,
  # prevent duplicate variable code entries by adding a `UNIQUE` constraint
  # to its relevant table column.
  if (!DBI::dbExistsTable(db_conn, "VARCODES")) {
    tbl_gen_query <-
      "CREATE TABLE VARCODES" %>%
      paste("(ID INTEGER PRIMARY KEY AUTOINCREMENT, VARCODE TEXT UNIQUE)")
    DBI::dbExecute(db_conn, tbl_gen_query)
  }

  # Fetch the variable codes already in the database (just the codes, not the IDs)
  old_varcodes <- fetch_varcodes(db_conn)$VARCODE
  new_varcodes <- varcodes %>% subset(. %not_in% old_varcodes)

  # `ID` values are automatically assigned by the database engine to variable
  # codes, so here we assign `NA` values to them before sending the table to
  # the database for writing.
  if (length(new_varcodes) == 0) return()
  rlog::log_info(glue::glue(
    "Assigning database IDs to {length(new_varcodes)} new variable codes"
  ))
  DBI::dbWriteTable(
    db_conn, "VARCODES",
    tibble::tibble(
      ID = rep(NA, length(new_varcodes)),
      VARCODE = new_varcodes
    ),
    append = TRUE)
}

#' Retrieve variable names from an index table
#'
#' `fetch_varcodes()` retrieves a character vector containing all the variable
#' codes that have thus far been assigned database ID values with `write_varcodes()`
#'
#' Given the numerous variables in the source data, this library pivots the data
#' into long form before writing to the database. This offers the benefit of
#' allowing each table in the database to have a predictable description and
#' prevent issues with exceeding the native maximum column support of many
#' database engines.
#'
#' However, pivoting the data creates a text column with a copy of the variable
#' code being stored for every one of the millions of observations in the data
#' set, significantly expanding the storage requirement of the database, and
#' potentially making queries run noticeably slower on standard spinning-platter
#' hard drives due to the increased scanning time.
#'
#' This function is part of an effort to alleviate that problem by assigning
#' each variable code an integer ID value so that the storage requirements are
#' significantly smaller.
#'
#' @param db_conn An active database connection created by `db_connector_*()`.
#' If `NULL`, the default database connection will be activated.
#' @return A character vector of variable codes
#' @export
fetch_varcodes <- function(db_conn = NULL) {
  was.null <- FALSE
  
  if (is.null(db_conn)) {
    was.null = TRUE
    db_connector <- db_connector_sqlite()
    db_conn <- db_connector()
  }
  
  if (!DBI::dbExistsTable(db_conn, "VARCODES")) {
    return(as.character(NULL))
  }
  
  df_varcodes <- 
    DBI::dbReadTable(db_conn, "VARCODES") %>% 
    dplyr::collect()
  
  if (was.null) DBI::dbDisconnect(db_conn)
  
  return(df_varcodes)
}

#' Convert a quarter ID back into a date
#'
#' The Chicago Fed and FFIEC source data altogether run quarterly from the
#' first quarter of 1976 to the present (roughly, the last quarter that ended
#' roughly at least 6 weeks ago). This function converts either a `Date` or a
#' string containing a date in ISO `YYYY-MM-DD` format into a small integer ID
#' value, where `1976-Q1` is represented by `1` and incrementing from there.
#'
#' Pivoting the source data into long form before writing it to the database
#' has the benefit of giving the database tables a simple and comprehensible
#' structure, but requires each date to be written thousands or millions of
#' times due to the sheer number of bank-variable pairs each period.
#'
#' Creating an integer ID to index the dates allows the date column in each
#' table to be stored as an integer, which requires significantly less space (2
#' bytes max) than even the most conservative string format that keeps the date
#' readable (8 bytes). This significantly reduces the storage requirement of the
#' extracted data sets, and potentially improves query performance when the
#' database is kept on slower storage devices such as spinning-platter hard
#' drives.
#'
#' The extraction functions found in this library use this function's inverse,
#' `date_str_to_qtr_id()`, to convert the quarter-end reporting dates found in
#' the source data into integer-valued quarter IDs.
#'
#' @param qtr_int An `integer` representing the index of a quarter since the
#' beginning of year 1976
#' @return The `Date` of the last day of the requested quarter.
#' @export
qtr_id_to_date_str <- function(qtr_int) {
  yyyy <- 1976 + (qtr_int %/% 4)
  qq <- ifelse(qtr_int %% 4 == 0, 4, qtr_int %% 4)
  next_qtr_start <-
    glue::glue("{yyyy}-{qq}") %>%
    lubridate::yq() %>%
    lubridate::ceiling_date("quarter")
  this_qtr_end <- next_qtr_start - lubridate::days(1)
  return(this_qtr_end)
}

#' Convert a reporting date into an integer-valued quarter ID
#'
#' The Chicago Fed and FFIEC source data altogether run quarterly from the
#' first quarter of 1976 to the present (roughly, the last quarter that ended
#' roughly at least 6 weeks ago). This function converts either a `Date` or a
#' string containing a date in ISO `YYYY-MM-DD` format into a small integer ID
#' value, where `1976-Q1` is represented by `1` and incrementing from there.
#'
#' Pivoting the source data into long form before writing it to the database
#' has the benefit of giving the database tables a simple and comprehensible
#' structure, but requires each date to be written thousands or millions of
#' times due to the sheer number of bank-variable pairs each period.
#'
#' Creating an integer ID to index the dates allows the date column in each
#' table to be stored as an integer, which requires significantly less space (2
#' bytes max) than even the most conservative string format that keeps the date
#' readable (8 bytes). This significantly reduces the storage requirement of the
#' extracted data sets, and potentially improves query performance when the
#' database is kept on slower storage devices such as spinning-platter hard
#' drives.
#'
#' Query functions found in this library use this function's inverse,
#' `qtr_id_to_date_str()`, which converts quarter IDs back into readable dates
#' for output to the end user.
#'
#' @param date_str A `Date` or character value in ISO `YYYY-MM-DD` format
#' @return The index of the quarter since the beginning of the year 1976, the
#' initial year of data available from the Chicago Federal Reserve.
#' @export
date_str_to_qtr_id <- function(date_str) {
  yyyy <- lubridate::year(lubridate::ymd(date_str))
  qq <- lubridate::quarter(lubridate::ymd(date_str))
  return(4 * (yyyy - 1976) + qq)
}
