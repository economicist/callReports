get_chifed_zip_dir <- function() {
  pkgconfig::get_config('chifed_zip_dir', fallback = set_chifed_zip_dir())
}

get_ffiec_zip_dir <- function() {
  pkgconfig::get_config('chifed_zip_dir', fallback = set_ffiec_zip_dir())
}

get_logging_dir <- function() {
  pkgconfig::get_config('logging_dir', fallback = set_logging_dir())
}

get_sqlite_file <- function() {
  pkgconfig::get_config('sqlite_file', fallback = set_sqlite_file())
}

set_chifed_zip_dir <- function(path = NULL) {
  if (is.null(path)) {
    prompt <- 'Select a folder containing ZIP files from the Chicago Fed:'
    path <- rstudioapi::selectDirectory(caption = prompt)
  }
  pkgconfig::set_config(chifed_zip_dir = path)
  pkgconfig::get_config('chifed_zip_dir')
}

set_ffiec_zip_dir <- function(path = NULL) {
  if (is.null(path)) {
    prompt <- 'Select a folder containing ZIP files from the FFIEC:'
    path <- rstudioapi::selectDirectory(caption = prompt)
  } 
  pkgconfig::set_config(ffiec_zip_dir = path)
  pkgconfig::get_config('ffiec_zip_dir')
}

set_logging_dir <- function(path = NULL) {
  if (is.null(path)) {
    prompt <- 'Select a folder to save extraction logs to:'
    path <- rstudioapi::selectDirectory(caption = prompt)
  }
  pkgconfig::set_config(logging_dir = path)
  pkgconfig::get_config('logging_dir')
}

set_sqlite_file <- function(path = NULL) {
  ext_filter <- 'SQLite Databases (*.db, *.db3, *.sqlite, *.sqlite3)'
  if (is.null(path)) {
    prompt <- 'Select an SQLite database to save extracted data to:'
    path <- rstudioapi::selectFile(filter = ext_filter, existing = FALSE)
  }
  pkgconfig::set_config(sqlite_file = path)
  pkgconfig::get_config('sqlite_file')
}