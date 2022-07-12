rm(list = ls(all = TRUE))
devtools::document()

chifed_balance_sheet <- 
  fetch_chifed_observations(
    'CHIFED.OBS_RCON',
    'RCON2170', 'RCON2950', 'RCON3210'
  )

ffiec_balance_sheet <-
  fetch_ffiec_observations(
    'FFIEC.OBS_RC', 
    'RCON2170', 'RCON2948', 'RCON3210'
  )

db_conn <- db_connector_sqlite()()
df_varcodes <- dplyr::collect(DBI::dbReadTable(db_conn, 'VARCODES'))
{
  db_results <-
    DBI::dbSendQuery(db_conn, paste(
      'SELECT * FROM "CHIFED.OBS_RIAS"',
      'WHERE VALUE GLOB "*[^0-9]*"'
    ))
  df_nonint <- 
    DBI::dbFetch(db_results) %>%
    tibble::as_tibble() %>%
    dplyr::filter(!stringr::str_detect(VALUE, '^-[[:digit:]]+$'))
  DBI::dbClearResult(db_results)
  df_nonint
}

df_scinot <-
  df_nonint %>%
  dplyr::mutate(VALUE = as.double(VALUE))

df_scinot %>%
  dplyr::distinct(VARCODE_ID) %>%
  dplyr::inner_join(df_varcodes, by = c('VARCODE_ID' = 'ID')) %>%
  dplyr::arrange(VARCODE)
