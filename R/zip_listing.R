#' List ZIP files containing Call Report data from the Chicago Fed
#' 
#' `list_chifed_zips()` expects the path of a folder containing the bulk data
#' files for 1976-2000 offered at the [Chicago Fed's website](https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data-complete-1976-2000) 
#'
#' @param chifed_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' @return A character vector containing the full paths of valid ZIP files
#' @export
#' @examples
#' list_chifed_zips('./zips-chifed')
list_chifed_zips <- function(chifed_zip_path = get_chifed_zip_path()) {
  matching_zips <- 
    list.files(chifed_zip_path,
               pattern = '(CALL|call)[[:digit:]]{4}-(ZIP|zip)\\.(ZIP|zip)',
               full.names = TRUE)
  
  # Put the year 2000 zips at the end so they're in order. The year is two
  # digits so just using `sort()` won't do it.
  zips_2000  <- matching_zips[stringr::str_detect(matching_zips, '(CALL|call)00')]
  zips_other <- matching_zips[!stringr::str_detect(matching_zips, '(CALL|call)00')]
  zips_reordered <- c(zips_other, zips_2000)
  return(zips_reordered)
}

#' List ZIP and schedule files containing FFIEC bulk call report data
#' 
#' `list_ffiec_zips_and_tsvs()` provides an easiy traversable data frame
#' full of zip-schedule pairs that can be used by `extract_ffiec_tsv()`
#' 
#' ZIP files containing the bulk data 2001-present offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#'
#' @param ffiec_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' @return A `tibble` containing the full paths of valid ZIP files in one column
#' paired with the schedule files contained therein in the other
#' @export
#' @examples
#' list_ffiec_zips_and_tsvs('./zips-ffiec')
list_ffiec_zips_and_tsvs <- function(ffiec_zip_path) {
  list_ffiec_zips(ffiec_zip_path) %>%
    purrr::map_dfr(function(zip_file) {
      tibble::tibble(zip_file = zip_file,
                     sch_file = list_ffiec_tsvs(zip_file))
    })
}

#' List ZIP files containing Call Report data from the FFIEC
#' 
#' `list_ffiec_zips()` lists valid ZIP files for extraction in a given directory.
#' 
#' ZIP files containing the bulk data 2001-present offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#'
#' @param ffiec_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' @return A character vector containing the full paths of valid ZIP files
#' @export
#' @examples
#' list_ffiec_zips('./zips-ffiec')
list_ffiec_zips <- function(ffiec_zip_path = get_ffiec_zip_path()) {
  rx_pattern <- '^FFIEC CDR Call Bulk All Schedules [[:digit:]]{8}\\.zip$'
  list.files(ffiec_zip_path, pattern = rx_pattern) %>%
    purrr::map_dfr(
      ~ tibble::tibble(report_date = 
                         stringr::str_extract(., '[[:digit:]]{8}') %>%
                         lubridate::mdy(),
                       filename    = paste0(ffiec_zip_path, '/', .))
    ) %>%
    dplyr::arrange(report_date) %>%
    getElement('filename')
}

#' List schedule files within an FFIEC Call Report ZIP
#' 
#' `list_ffiec_tsvs()` lists the schedule files contained within a given
#' ZIP file provided by the FFIEC.
#'
#' ZIP files containing the bulk data 2001-present offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#'
#' @param zip_file A ZIP file containing bulk data provided by the FFIEC
#' @return A `tibble` with the name of  containing the schedule filenames contained in `zip_file`
#' @export
#' @examples
#' list_ffiec_tsvs('FFIEC CDR Call Bulk All Schedules 06302004.zip')
list_ffiec_tsvs <- function(zip_file = rstudioapi::selectFile()) {
  rx <- paste0('FFIEC CDR Call Schedule [[:alpha:]]+ [[:digit:]]{8}',
               '(\\([[:digit:]] of [[:digit:]]\\))*\\.txt')
  unzip(zip_file, list = TRUE) %>%
    getElement('Name') %>%
    subset(stringr::str_detect(., rx)) %>%
    subset(!stringr::str_detect(., 'CI|GCI|GI|GL'))
}