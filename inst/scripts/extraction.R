rm(list = ls(all.names = TRUE))
# library(callReports)
devtools::document()

##
## Below is all you need to run to get *ALL* of the Chicago Federal Reserve
## and FFIEC data sets extracted into a single database. The SQLite file is
## approximately 35 GB.
##

create_new_sqlite_db(overwrite = TRUE, confirm = FALSE)
extract_chifed_zips()
extract_ffiec_zips()
