if (interactive()) rm(list = ls(all = TRUE))
if (commandArgs()[1] == 'RStudio') {
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
}

# Uncomment one *and only one* of these two lines. If you change your mind, 
# start a new session with `Session > Terminate R...` in the menu bar if you are
# using RStudio. `Session > Restart R` is NOT a reliable way to ensure a library
# is unloaded, so use `Session > Terminate R...` to ensure that you know which
# library a given run of your script is using.

#library(callReports)
devtools::load_all('~/code/callReports') # use path to saved/cloned library

# Specify the paths of your desired SQLite database file and the folder where
# you've stored the ZIP files downloaded from the FFIEC. `db_connector_sqlite()`
# in the last line below creates a `function` that gets called from within the
# extraction algorithm to open a new database connection for each schedule file
# it writes. This allows it to close the database connection after writing a 
# collection of extracted observations, and reopen the connection later instead
# of having one long extended connection.
ffiec_sqlite_file  <- '~/db/callreports/ffiec.sqlite'
ffiec_zip_folder   <- '~/data/callreports/ffiec'
ffiec_db_connector <- db_connector_sqlite(ffiec_sqlite_file, overwrite = TRUE)

# If you interrupt the extraction (by hitting the `STOP` button or `ESC` key
# inside the console while it's running), there is a chance you'll do it in the
# middle of a write cycle to the database. Unfortunately, "user interrupts" seem
# to be something that can't be handled by the `tryCatch()` block the database
# writing operation is in, so in that event the database may be locked and 
# unreadable by queries from `R` and external applications, such as the SQLite3
# command line interpreter or SQLiteBrowser, until the open connection to it is
# closed. `R` shows an error in the console saying that the database is locked
# in this event. If that happens, and you'd still like to browse what's been
# extracted thus far either with the query tools in the library here or any
# external application, run the next line on its own to close ALL open database
# and file connections associated with this instance of R/RStudio.
closeAllConnections()

# It's useful to have a log of any major extraction operation, as the RStudio
# console and many terminal emulators have default buffers that are far too
# short to scroll back through for inspection. The `callReports` library has
# a function called `generate_log_name()` to assist in creating a log filename
# with a built-in timestamp in case you want to save the log of every run.
capture.output(extract_all_ffiec_zips(ffiec_db_connector, ffiec_zip_folder),
               file  = generate_log_name('logs/extraction_attempt'),
               split = TRUE) # (allows for output in the console as well)
