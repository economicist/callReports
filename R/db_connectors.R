#' Create a `function` that opens an SQLite connection on command.
#' 
#' `db_connector_sqlite()` returns a `function` that allows an SQLite connection
#' to be opened whenever necessary by other functions in this package.
#' 
#' @param sqlite_db_path The path to an SQLite database file. Must be in an
#' existing directory. The file will be created if it does not exist.
#' @return A `function` that opens a connection to the database given in
#' `sqlite_db_path`. It should be passed to other functions without the `()`
#' @importFrom magrittr %>%
#' @importFrom DBI dbConnect
#' @importFrom glue glue
#' @export
#' @examples
#' db_connector_sqlite('./db/ffiec.sqlite')
db_connector_sqlite <- function(sqlite_db_path, overwrite = FALSE) {
  dir_name  <- dirname(sqlite_db_path)
  dir_name  <- ifelse(dir_name == '.', getwd(), dir_name)
  file_name <- basename(sqlite_db_path)
  
  if (!dir.exists(dir_name)) {
    stop(glue('Directory `{dir_name}` does not exist. Confirm or create it.'))
  }
  
  if (file.exists(sqlite_db_path)) {
    journal_path <- glue('{sqlite_db_path}-journal')
    if (file.exists(journal_path)) {
      cat(glue('Database lock file {journal_path} detected.'), '\n')
      cat('This can be because there is an active transaction being performed\n')
      cat('on the database, or perhaps because a transaction was interrupted.\n')
      confirm_and_delete(journal_path)
    }
    if (overwrite) confirm_and_delete(sqlite_db_path)
  } else {
    cat(glue('`{file_name}` does not exist in directory `{dir_name}`'), '\n')
    cat('Attempting to create it...\n')
    tryCatch(callReports::create_new_sqlite_db(sqlite_db_path), error = stop)
  }
  
  function() dbConnect(RSQLite::SQLite(), sqlite_db_path)
}

#' Create a new SQLite database to extract data into
#' 
#' If the SQLite database filename specified in `sqlite_db_path` already exists
#' and `overwrite` is set to `FALSE`, the user will be prompted to explicitly
#' permit overwriting it it.
#' 
#' `db_connector_sqlite(sqlite_db_path)` requires the path to a valid existing
#' SQLite database file. If one does not exist, one should be created using this
#' function before proceeding to extract data.
#' 
#' @param sqlite_db_path A path containing at least a valid SQLite filename. It
#' will be created in the current working directory if is not specified as part
#' of the filename.
#' @return A `function` that opens a connection to the newly created SQLite
#' database on demand
#' @importFrom DBI dbConnect dbDisconnect
#' @importFrom glue glue
#' @importFrom rlog log_info
#' @export
#' @examples
#' # Create a new database file:
#' create_new_sqlite_db('./zips-ffiec/ffiec.sqlite')
create_new_sqlite_db <- function(sqlite_db_path) {
  # If file exists, ask for permission to overwrite. Exit if permission denied.
  callReports::confirm_and_delete(sqlite_db_path)
  
  # Try to open a connection to a new SQLite database at `sqlite_db_path`.
  # Stop execution if there's an error. Announce success otherwise.
  tryCatch({
    db_conn <- dbConnect(RSQLite::SQLite(), sqlite_db_path)
    dbDisconnect(db_conn)
  },
  error = stop)
  log_info(glue('New SQLite database `{sqlite_db_path}` created.'))
  cat('\n')
}