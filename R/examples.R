#' List the available example scripts for this package
#'
#' For user convenience, a few sample scripts are included with this package 
#' which can be opened using `open_example_script()`. This function lists the
#' available sample scripts.
#'
#' @return A list of sample scripts available for opening with `open_example_script()`
#' @export
list_example_scripts <- function() {
  script_dir <- system.file('scripts/', package = 'callReports')
  list.files(script_dir)
}

#' Copy an example script to the location of your choice and open it.
#'
#' For user convenience, a few sample scripts are included with this package
#' which can be listed using `list_example_scripts()`. This function copies
#' the script selected by the user
#'
#' @param script One of the scripts listed by `list_example_scripts()`
#' @param copy (default `TRUE`) Copy the file to a writable location? If `FALSE`
#' the copy of the script stored with the package will be opened. You should be
#' asked for a different location to save it to if you edit it.
#' @export
copy_example_script <- function(script = NULL, open = TRUE) {
  script_dir <- system.file('scripts/', package = 'callReports')
  script_path <- ifelse(
    !is.null(script),
    file.path(script_dir, script),
    rstudioapi::selectFile(
      caption = "Please select an example script to copy and open",
      path = script_dir
    )
  )

  working_dir <- ifelse(
    !is.null(rstudioapi::getActiveProject()),
    rstudioapi::getActiveProject(),
    getwd()
  )
  
  dest_dir <- rstudioapi::selectDirectory(
    caption = "Please select a location to save the requested example script",
    label = "Save Here",
    path = working_dir
  )
  
  dest_path <- file.path(dest_dir, basename(script_path))
  
  script_lines <- vroom::vroom_lines(script_path)
  vroom::vroom_write_lines(script_lines, dest_path)
  
  if (open) rstudioapi::navigateToFile(dest_path)
}