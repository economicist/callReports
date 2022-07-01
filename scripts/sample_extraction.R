if (interactive()) rm(list = ls(all = TRUE))
if (commandArgs()[1] == 'RStudio') {
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
}

# library(callReports)
devtools::document()

chifed_zip_folder <- '~/data/callreports/chifed'
ffiec_zip_folder  <- '~/data/callreports/ffiec'
sqlite_file       <- '~/db/sqlite/callreports.sqlite'
sqlite_connector  <- db_connector_sqlite(sqlite_file)

if (!dir.exists('./logs')) dir.create('./logs')
closeAllConnections() # Close any lingering open database connections.
capture.output(extract_all_chifed_zips(sqlite_connector, chifed_zip_folder),
               file  = generate_log_name('logs/extraction_chifed'),
               split = TRUE) # (allows for output in the console as well)
capture.output(extract_all_ffiec_zips(sqlite_connector, ffiec_zip_folder),
               file  = generate_log_name('logs/extraction_ffiec'),
               split = TRUE) # (allows for output in the console as well)