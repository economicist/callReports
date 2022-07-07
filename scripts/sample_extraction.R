if (rstudioapi::isAvailable()) {
  rm(list = ls(all = TRUE))
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
}

# library(callReports)
devtools::document('~/code/callReports/')

set_chifed_zip_dir()
extract_chifed_zips(db_connector_sqlite(), get_chifed_zip_dir())

set_ffiec_zip_dir()
extract_ffiec_zips(db_connector_sqlite(), get_ffiec_zip_dir())
