if (rstudioapi::isAvailable()) {
  rm(list = ls(all = TRUE))
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
}

# library(callReports)
devtools::document('~/code/callReports/')

extract_chifed_zips(db_connector_sqlite(), get_chifed_zip_dir())
extract_ffiec_zips(db_connector_sqlite(), get_ffiec_zip_dir())