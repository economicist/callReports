rm(list = ls(all.names = TRUE))
library(callReports)

# Latest feature: Persistent configuration with interactive selections
#
# Not necessary to run the following `setter` functions the first time you use
# this library, as the `getter` functions will automatically call them when they
# need values to get.
#
# Do, however, uncomment and run the appropriate line below if you ever need to
# change the location or name of the database file, or either of the folders
# where the ZIP files are stored:

# set_sqlite_filename()
# set_chifed_zip_dir()
# set_ffiec_zip_dir()


# The `get_*()` functions used as arguments below can all be used on their own
# if you ever want to know the current database and ZIP folder configuration.

create_new_sqlite_db(get_sqlite_filename(), overwrite = TRUE, confirm = FALSE)
# extract_chifed_zips(db_connector_sqlite(), get_chifed_zip_dir())
extract_ffiec_zips(db_connector_sqlite(), get_ffiec_zip_dir())
