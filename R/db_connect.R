#' Create a `function` that opens an SQLite connection on command.
#' 
#' `db_connector_sqlite()` returns a `function` that allows an SQLite connection
#' to be opened whenever necessary by other functions in this package.
#' 
#' @param sqlite_path The path to an SQLite database file. Must be in an
#' existing directory. The file will be created if it does not exist.
#' @return A `function` that opens a connection to the database given in
#' `sqlite_path`. It should be passed to other functions without the `()`
#' @export
#' @examples
#' db_connector_sqlite('./db/ffiec.sqlite')
db_connector_sqlite <- 
  function(sqlite_path = get_sqlite_filename(),
           overwrite   = FALSE,
           confirm     = TRUE) {
    if (overwrite) {
      create_new_sqlite_db(sqlite_path, overwrite = TRUE, confirm = confirm)
    }
    function() DBI::dbConnect(RSQLite::SQLite(), sqlite_path)
  }

#' Create a new SQLite database to extract data into
#' 
#' If the SQLite database filename specified in `sqlite_path` already exists
#' and `overwrite` is set to `FALSE`, the user will be prompted to explicitly
#' permit overwriting it it.
#' 
#' `db_connector_sqlite(sqlite_path)` requires the path to a valid existing
#' SQLite database file. If one does not exist, one should be created using this
#' function before proceeding to extract data.
#' 
#' @param sqlite_path A path containing at least a valid SQLite filename. It
#' will be created in the current working directory if is not specified as part
#' of the filename.
#' @return A `function` that opens a connection to the newly created SQLite
#' database on demand
#' @export
#' @examples
#' # Create a new database file:
#' create_new_sqlite_db('./zips-ffiec/ffiec.sqlite')
create_new_sqlite_db <- function(sqlite_path, overwrite = FALSE, confirm = TRUE) {
  # If file exists, ask for permission to overwrite. Exit if permission denied.
  journal_path <- glue::glue('{sqlite_path}-journal')
  if (file.exists(journal_path)) confirm_and_delete(journal_path)
  if (overwrite & file.exists(sqlite_path)) {
    if (confirm) {
      confirm_and_delete(sqlite_path)
    } else {
      unlink(sqlite_path)
    }
    
  }
  
  # Try to open a connection to a new SQLite database at `sqlite_path`.
  # Stop execution if there's an error. Announce success otherwise.
  tryCatch({
    db_conn <- DBI::dbConnect(RSQLite::SQLite(), sqlite_path)
    DBI::dbDisconnect(db_conn)
  },
  error = stop)
  rlog::log_info(glue::glue('New SQLite database `{sqlite_path}` created.'))
  cat('\n')
}

