if (interactive()) rm(list = ls(all = TRUE))
if (commandArgs()[1] == 'RStudio') {
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
}

#library(callReports)
devtools::document()

sqlite_file      <- '~/db/sqlite/callreports.sqlite'
sqlite_connector <- db_connector_sqlite(sqlite_file)

balance_sheet_rcfd_chifed <- 
  query_chifed_db(sqlite_connector, 'CHIFED.OBS_ALL', 
                  'RCFD2170', 'RCFD2948', 'RCFD2950', 'RCFD3210')

balance_sheet_rcfd_ffiec <-
  query_ffiec_db(sqlite_connector, 'FFIEC.OBS_RC',
                 'RCFD2170', 'RCFD2948', 'RCFD2950', 'RCFD3210')