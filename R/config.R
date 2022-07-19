#' Build the full path to a YAML configuration file
#'
#' This package keeps your settings for the database, ZIP folders,
#' and logging folder in a persistent configuration file located in your user
#' configuration directory as given by `rappdirs::user_config_dir()`, in an
#' optional subdirectory specified by the parameter `subdir_name`. This
#' function builds and returns the full path.
#'
#' @param subdir_name (default `economicist`) A subdirectory of your local
#' user configuration directory, in which to place the configuration file
#' given by `yaml_name`
#' @param yaml_name (default `callReports.yml`) A name for the configuration file
#' @return A full constructed file path to the configuration file given by
#' `subdir_name` and `yaml_name`
#' @export
get_cfg_filename <-
  function(subdir_name = "economicist", yaml_name = "callReports.yml") {
    list(
      user = file.path(rappdirs::user_config_dir(subdir_name), yaml_name),
      template = file.path(system.file('cfg/', package = "callReports"), yaml_name)
    )
  }

#' Delete all user configurations for this package
#'
#' @param subdir_name (default `economicist`) A subdirectory of your local
#' user configuration directory, in which to place the configuration file
#' given by `yaml_name`
#' @param yaml_name (default `callReports.yml`) A name for the configuration file
#' @export
reset_user_cfg <-
  function(subdir_name = "economicist", yaml_name = "callReports.yml") {
    if (file.exists(get_cfg_filename()$user)) unlink(get_cfg_filename()$user)
    create_user_cfg_if_not_exists(subdir_name, yaml_name)
    return(invisible())
  }

#' Create a configuration file for this package in the local user configuration
#' directory
#'
#' `create_user_cfg_if_not_exists()` uses `get_cfg_filename()` to determine the
#' location of the user configuration file for this package. If no configuration
#' file is found at that location, the template provided with the package is
#' copied to it.
#'
#' @param subdir_name (default `economicist`) A subdirectory of your local
#' user configuration directory, in which to place the configuration file
#' given by `yaml_name`
#' @param yaml_name (default `callReports.yml`) A name for the configuration file
#'
#' @export
create_user_cfg_if_not_exists <-
  function(subdir_name = "economicist", yaml_name = "callReports.yml") {
    if (!file.exists(get_cfg_filename()$user)) {
      cfg_subdir <- file.path(rappdirs::user_config_dir(), subdir_name)
      if (!dir.exists(cfg_subdir)) dir.create(cfg_subdir)
      tryCatch(file.copy(get_cfg_filename()$template, get_cfg_filename()$user),
        warning = warning, error = stop
      )
    }
  }

#' Get the directory where the Chicago Fed ZIP files are stored
#'
#' @return A directory character value
#' @export
get_chifed_zip_dir <- function() {
  create_user_cfg_if_not_exists()
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  cfg_val <- cfg_list$zips_chifed
  if (is.null(cfg_val)) cfg_val <- set_chifed_zip_dir()
  return(cfg_val)
}

#' Get the directory where the FFIEC ZIP files are stored
#'
#' @return A directory character value
#' @export
get_ffiec_zip_dir <- function() {
  create_user_cfg_if_not_exists()
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  cfg_val <- cfg_list$zips_ffiec
  if (is.null(cfg_val)) {
    return(set_ffiec_zip_dir())
  }
  return(cfg_val)
}

#' Get the directory where the extraction and query logs are stored
#'
#' @return A directory character value
#' @export
get_logging_dir <- function() {
  create_user_cfg_if_not_exists()
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  cfg_val <- cfg_list$logging_dir
  if (is.null(cfg_val)) {
    return(set_logging_dir())
  }
  return(cfg_val)
}

#' Get the name of the SQLite database
#'
#' @return A directory character value
#' @export
get_sqlite_filename <- function() {
  create_user_cfg_if_not_exists()
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  cfg_val <- cfg_list$sqlite_file
  if (is.null(cfg_val)) {
    return(set_sqlite_filename())
  }
  return(cfg_val)
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
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  cfg_list$zips_chifed <- path
  yaml::write_yaml(cfg_list, get_cfg_filename()$user)
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  return(cfg_list$zips_chifed)
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
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  cfg_list$zips_ffiec <- path
  yaml::write_yaml(cfg_list, get_cfg_filename()$user)
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  return(cfg_list$zips_ffiec)
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
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  cfg_list$logging_dir <- path
  yaml::write_yaml(cfg_list, get_cfg_filename()$user)
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  return(cfg_list$logging_dir)
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
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  cfg_list$sqlite_file <- path
  yaml::write_yaml(cfg_list, get_cfg_filename()$user)
  cfg_list <- yaml::read_yaml(get_cfg_filename()$user)
  return(cfg_list$sqlite_file)
}
