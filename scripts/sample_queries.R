rm(list = ls(all = TRUE))
devtools::document()

fetch_ffiec_observations(db_connector_sqlite(), 'FFIEC.OBS_RC',
                         'RCON2170', 'RCON2948', 'RCON3210')