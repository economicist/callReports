if (interactive()) rm(list = ls(all = TRUE))
if (commandArgs()[1] == 'RStudio') {
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
}

library(magrittr)
library(tidyverse)

if (file.exists('../callReports.Rproj')) {
  devtools::load_all('..')
} else {
  if (!nzchar(system.file(package = 'callReports'))) {
    devtools::install_github('economicist/callReports')
  }
  library(callReports)
}

# Don't post a message about the column types imported -- we need to convert
# everything to character to be pivotable.
options(readr.show_col_types = FALSE)

# unlink('./db/ffiec.sqlite')
ffiec_db <- db_connector_sqlite('~/db/callreports/ffiec.sqlite', overwrite = TRUE)
extract_all_ffiec_zips(ffiec_db, '~/data/callreports-zips-ffiec')

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