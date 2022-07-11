##
## These are all called automatically the first time you use their corresponding
## `get_*()` functions (or, more likely, a behind-the-scenes function that calls
## one), so you can leave them commented out until you need one:
##

set_sqlite_filename()
set_chifed_zip_dir()
set_ffiec_zip_dir()
set_logging_dir()

##
## You can nuke all settings associated with this package with one command, and
## you'll be asked to respecify all settings when necessary:
##

reset_user_cfg()
