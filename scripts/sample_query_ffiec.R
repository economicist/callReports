if (interactive()) rm(list = ls(all = TRUE))
if (commandArgs()[1] == 'RStudio') {
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
}

library(magrittr)
library(tidyverse)

# Uncomment one *and only one* of these two lines. If you change your mind, 
# start a new session with `Session > Terminate R...` in the menu bar if you are
# using RStudio. `Session > Restart R` is NOT a reliable way to ensure a library
# is unloaded, so use `Session > Terminate R...` to ensure that you know which
# library a given run of your script is using.

#library(callReports)
devtools::load_all('~/code/callReports')

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

# Here is a sample query that requests a variety of variables related to total
# assets, liabilities, and equity capital. It took about 5 minutes when I ran
# it on a laptop with an i5-6300U processor (a processor optimized for low
# electricity usage and absolutely not performance).
#
# The Chicago Fed data is all in one table, so it's really easy to query all
# the variables you want without having to bother with joins or anything...

# The FFIEC query tool requires the second argument to be the name of the table
# you want to get variables out of. The third and later arguments represent the
# variables you want from that table.
#
# Anyway, here's a sample query for the FFIEC data, returning results similar
# to the Chicago query:
df_sample_query <- 
  query_ffiec_db(ffiec_db, 'RC', 
                 'RCFD2170', 'RCON2170',
                 'RCFD2948', 'RCFD2950', 'RCON2948', 'RCON2950', 
                 'RCFD3210', 'RCONG105', 'RCON3210')

# Here we can see that there is in fact only one record for each (ID, DATE).
# This means we don't have to do anything crazy with the results.
df_multiple_rows_per_obs <-
  df_sample_query %>%
  group_by(IDRSSD, REPORT_DATE) %>%
  filter(n() > 1)