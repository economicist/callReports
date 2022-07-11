rm(list = ls(all.names = TRUE))
# library(callReports)
devtools::document()

##
## These are all called automatically the first time you use their corresponding
## `get_*()` functions (or, more likely, a behind-the-scenes function that calls
## one), so you can leave them commented out until you need one:
##
# set_sqlite_filename()
# set_chifed_zip_dir()
# set_ffiec_zip_dir()
# set_logging_dir()

##
## You can nuke all settings associated with this package with one command, and
## you'll be asked to respecify all settings when necessary:
##
# reset_user_cfg()

##
## Below is all you need to run to get *ALL* of the Chicago Federal Reserve
## and FFIEC data sets extracted into a single database. The SQLite file is
## approximately 35 GB.
##
options(warn = 1)
create_new_sqlite_db(overwrite = TRUE, confirm = FALSE)
# extract_chifed_zips()
extract_ffiec_zips()
