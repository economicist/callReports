#' Build the full path to a YAML configuration file
#'
#' This package keeps your settings for the database, ZIP folders,
#' and logging folder in a persistent configuration file located in your user
#' configuration directory as given by `rappdirs::user_config_dir()`, in an
#' optional subdirectory specified by the parameter `subdir_name`. This
#' function builds and returns the full path.
#'
#' @param subdir_name (default `economicist`) A subdirectory within your local
#' user configuration directory
#' @param yaml_name (default `callReports.yml`) A name for this package's
#' configuration file
#' @return A constructed file path
#' @export
build_config_filename <-
  function(subdir_name = "economicist", yaml_name = "callReports.yml") {
    cfg_dir <- rappdirs::user_config_dir(subdir_name)
    cfg_file <- file.path(cfg_dir, yaml_name)
    return(cfg_file)
  }



#' Fetch selected configuration settings for the package
#'
#' @param ... The character-valued names of the configuration settings you wish
#' to retrieve
#' @param cfg_file The path to a YAML configuration file containing the settings
#' given in `...`
#' @return A named list with the values of the requested configuration settings
#' @export
get_config_setting <-
  function(key, cfg_file = build_config_filename()) {
    cfg_list <- yaml::read_yaml(cfg_file)
    if (...length() == 0) {
      return(cfg_list)
    }

    cfg_keys <- names(c(...))
    cfg_keys <- subset(cfg_keys, cfg_keys %in% names(cfg_list))
    return(cfg_list[cfg_keys])
  }

put_config_setting <-
  function(..., cfg_file = build_config_filename()) {
    if (...length() != 1) {
      stop("`put_config_setting()` requires a single named parameter.")
    }
    cfg_list <- yaml::read_yaml(cfg_file)
  }

#' Update the package configuration with new parameters
#'
#' @param cfg_file The path to a YAML configuration file.
#' @return The updated package settings
#' @export
update_cr_config_settings <-
  function(..., cfg_file = build_config_filename()) {
    cfg_list <- yaml::read_yaml(cfg_file)
    cfg_list <- modifyList(cfg_list, list(...))
    yaml::write_yaml(cfg_list, cfg_file)
    return(cfg_list)
  }

#' Get the directory where the Chicago Fed ZIP files are stored
#'
#' @return A directory character value
#' @export
get_chifed_zip_dir <- function() {
  pkgconfig::get_config("chifed_zip_dir", fallback = set_chifed_zip_dir())
}

#' Get the directory where the FFIEC ZIP files are stored
#'
#' @return A directory character value
#' @export
get_ffiec_zip_dir <- function() {
  pkgconfig::get_config("ffiec_zip_dir", fallback = set_ffiec_zip_dir())
}

#' Get the directory where the extraction and query logs are stored
#'
#' @return A directory character value
#' @export
get_logging_dir <- function() {
  pkgconfig::get_config("logging_dir", fallback = set_logging_dir())
}

#' Get the name of the SQLite database
#'
#' @return A directory character value
#' @export
get_sqlite_filename <- function() {
  pkgconfig::get_config("sqlite_file", fallback = set_sqlite_filename())
}

#' Set the directory where the Chicago Fed ZIP files are stored
#'
#' @return A directory character value
#' @export
set_chifed_zip_dir <- function(path = NULL) {
  if (is.null(path)) {
    prompt <- "Select a folder containing ZIP files from the Chicago Fed:"
    path <- rstudioapi::selectDirectory(caption = prompt)
  }
  pkgconfig::set_config(chifed_zip_dir = path)
  pkgconfig::get_config("chifed_zip_dir")
}

#' Set the directory where the FFIEC ZIP files are stored
#'
#' @return A directory character value
#' @export
set_ffiec_zip_dir <- function(path = NULL) {
  if (is.null(path)) {
    prompt <- "Select a folder containing ZIP files from the FFIEC:"
    path <- rstudioapi::selectDirectory(caption = prompt)
  }
  pkgconfig::set_config(ffiec_zip_dir = path)
  pkgconfig::get_config("ffiec_zip_dir")
}

#' Get the directory where the extraction and query logs are stored
#'
#' @return A directory character value
#' @export
set_logging_dir <- function(path = NULL) {
  if (is.null(path)) {
    prompt <- "Select a folder to save extraction logs to:"
    path <- rstudioapi::selectDirectory(caption = prompt)
  }
  pkgconfig::set_config(logging_dir = path)
  pkgconfig::get_config("logging_dir")
}

#' Get the name of the SQLite database
#'
#' @return A directory character value
#' @export
set_sqlite_filename <- function(path = NULL) {
  ext_filter <- "SQLite Databases (*.db, *.db3, *.sqlite, *.sqlite3)"
  if (is.null(path)) {
    prompt <- "Select or create an SQLite database for extracted data:"
    path <- rstudioapi::selectFile(
      caption = prompt,
      filter = ext_filter,
      existing = FALSE
    )
  }
  pkgconfig::set_config(sqlite_file = path)
  pkgconfig::get_config("sqlite_file")
}
