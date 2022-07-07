#' Extract an entire directory of FFIEC ZIP files to a database
#'
#' `extract_ffiec_zips_to_db()` examines all ZIP files in `ffiec_zip_path`
#' and attempts to extract each enclosed schedule file to the database whose
#' connector function is given by `db_connector`. The underlying extraction
#' function skips schedule files that have already been successfully extracted
#' to the database.
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param ffiec_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' @return NULL
#' @export
#' @examples
#' db_connector <- db_connector_sqlite('./db/ffiec.sqlite')
#' extract_ffiec_zips_to_db(db_connector, './zips-ffiec')
extract_ffiec_zips <- 
  function(db_connector   = get_db_connector, 
           ffiec_zip_path = get_ffiec_zip_dir()) {
    closeAllConnections()
    capture.output({
      list_ffiec_zips_and_tsvs(ffiec_zip_path) %>%
        purrr::pwalk(function(zip_file, sch_file) {
          extract_ffiec_tsv(db_connector, zip_file, sch_file)
        })
    },
    file  = generate_log_name('extraction_ffiec'),
    split = TRUE)
  }

#' Extract the FFIEC codebook and all observations for a single schedule
#' 
#' `extract_ffiec_tsv()` accepts a ZIP file, extracts one of its enclosed
#' tab-separated-value schedule files, and writes the extracted data to a given
#' database.
#' 
#' ZIP files containing the bulk data 2001-present offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#' 
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param zf A ZIP file containing schedule files
#' @param sch A valid FFIEC schedule data file inside the ZIP file `zf`
#' @export
#' @examples
#' The database connector only needs to be created once in any given script.
#' ffiec_db <- sqlite_connector('./db/ffiec.sqlite')
#' extract_ffiec_tsv(
#'   ffiec_db,
#'   './zips-ffiec/FFIEC FFIEC CDR Call Bulk All Schedules 03312018.zip',
#'   'FFIEC CDR Call Schedule RCCII 06302002.txt'
#' )
extract_ffiec_tsv <- function(db_connector, zf, sch) {
  # Unzip the schedule file and get some basic metadata for the codebook
  unzip(zf, sch, exdir = tempdir(), unzip = getOption('unzip'))
  sch_unzipped <- paste0(tempdir(), '/', sch)
  sch_file     <- basename(sch_unzipped)
  
  sch_info     <- read_ffiec_filename(sch_file)
  report_date  <- sch_info['report_date']
  sch_code     <- sch_info['sch_code']
  part_str     <- 
    ifelse(sch_info[4] != "1", glue::glue('({sch_info[3]} of {sch_info[4]})'), '')
  rlog::log_info(glue::glue('{report_date} {sch_code} {part_str}'))
  
  # Move on if we've already extracted this one. A schedule file's codebook
  # record is written to the database only if the respective observations are 
  # successfully extracted and written. We can thus quickly determine whether
  # the schedule is already in the database, without a full query of the
  # observation table, by consulting the codebook.
  scheds_in_db <- ffiec_schedules_in_db(db_connector)
  db_not_empty <- nrow(scheds_in_db) > 0

  if (db_not_empty) {
    matching_sch_in_db <- dplyr::filter(scheds_in_db,
                                 REPORT_DATE   == report_date,
                                 SCHEDULE_CODE == sch_code,
                                 PART_NUM      == sch_info['part_num'],
                                 PART_OF       == sch_info['part_of'])
    if (nrow(matching_sch_in_db) == 1) {
      rlog::log_info('Schedule already in database. Moving to next...')
      cat('\n')
      return()
    }
  }
  
  # If you've gotten this far in the function, the database has no records
  # associated with this schedule file. Attempt to extract the observations
  # and codebook information. Once it's extracted, take the variable names and
  # add them to the `VAR_CODES` table in the database, where each VAR_CODE will
  # be assigned a simple integer value for more efficient storage in the main
  # observation tables.
  df_codes  <- read_ffiec_codebook(sch_unzipped)
  var_codes <- unique(df_codes$VAR_CODE)
  write_varcodes(db_connector, var_codes)
  
  # Attempt to extract the observations for this file. If a warning is issued 
  # that is not explicitly absorbed and reconciled by the observation extraction
  # function, a visible warning log will be shown here.
  db_conn <- db_connector()
  df_varcodes <- 
    DBI::dbReadTable(db_conn, 'VAR_CODES') %>%
    dplyr::filter(VAR_CODE %in% var_codes) %>%
    dplyr::collect()
  df_obs <- 
    parse_ffiec_tsv(sch_unzipped) %>%
    tidyr::pivot_longer(names_to  = 'VAR_CODE',
                 values_to = 'VALUE',
                 cols = !any_of(c('IDRSSD')),
                 values_drop_na = TRUE) %>%
    dplyr::mutate(IDRSSD     = as.integer(IDRSSD),
           QUARTER_ID = as.integer(date_str_to_qtr_id(report_date))) %>%
    dplyr::inner_join(df_varcodes, by = 'VAR_CODE') %>%
    dplyr::rename(VAR_CODE_ID = ID) %>%
    dplyr::select(IDRSSD, QUARTER_ID, VAR_CODE_ID, VALUE)
  df_summ  <- 
    tibble::tibble(REPORT_DATE   = report_date,
                   SCHEDULE_CODE = sch_code,
                   PART_NUM      = sch_info[3],
                   PART_OF       = sch_info[4],
                   N_OBS         = ifelse(is.null(df_obs), 0, nrow(df_obs)))
  
  write_ffiec_schedule(db_connector, sch_code, df_obs, df_codes, df_summ)
  unlink(sch_unzipped)
}

#' Extract the observations for a given FFIEC schedule file to a database
#'
#' `parse_ffiec_tsv()` accepts a text file containing tab-separated-value
#' bulk data provided by the FFIEC, and writes it to a given database.
#' 
#' If any warnings are triggered about parsing issues, they are absorbed by the
#' function and an attempt is made to repair the schedule file. A second attempt
#' is then made, and a visible warning issued it was not resolved. Any data
#' extracted will still be written to the database, but you may wish to
#' investigate their integrity. Tests existing data sets as of 2022 June 17
#' have been working without warnings.
#'
#' Most applications will not use this function as it is called by
#' `extract_ffiec_tsv()` as part of extracting the codebook and observations
#' simultaneously.
#'
#' ZIP files containing valid schedule files for 2001-present are offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#' 
#' @param sch_unzipped A valid FFIEC schedule data file extracted to disk#'
#' @export
#' @examples 
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- db_connector_sqlite('./db/ffiec.sqlite')
#' parse_ffiec_tsv(ffiec_db, 'FFIEC CDR Call Schedule RCCII 06302002.txt')
parse_ffiec_tsv <- function(sch_unzipped) {
  sch_file    <- basename(sch_unzipped)
  sch_code    <- read_ffiec_sch_code(sch_unzipped)
  report_date <- parse_mdy_substring(sch_unzipped)
  rlog::log_info(glue::glue('Extracting observations...'))
  
  parse_attempt <- function(sch_unzipped) {
    sch_file <- basename(sch_unzipped)
    rlog::log_info(glue::glue('Parsing observations in {sch_file}'))
    
    old_coltype_option <- getOption('readr.show_col_types')
    options(readr.show_col_types = FALSE)
    df_out <-
      readr::read_tsv(sch_unzipped,
                      skip = 2,
                      na = c('', 'NA', 'NR', 'CONF'),
                      col_names  = read_ffiec_varcodes(sch_unzipped),
                      col_types  = readr::cols(.default = readr::col_character()),
                      name_repair = ~ repair_colnames(.),
                      col_select = !matches('UNNAMED_'),
                      progress   = FALSE) %>%
      dplyr::rename(IDRSSD = `"IDRSSD"`) %>%
      dplyr::select(!any_of('RCON9999')) %>%
      dplyr::mutate(IDRSSD = as.character(IDRSSD))
    options(readr.show_col_types = old_coltype_option)
    return(df_out) 
  }
  
  df_obs <- tryCatch(
    df_obs <- parse_attempt(sch_unzipped),
    warning = function(w) {
      rlog::log_info(
        'Warning issued trying to extract. Running repair function...')
      sch_fixed <- repair_ffiec_schedule(sch_unzipped)
      tryCatch(
        df_obs <- parse_attempt(sch_fixed),
        warning = function(w) {
          rlog::log_info('Warning still issued after repairing. You may wish to')
          rlog::log_info(glue::glue(
            'investigate table {sch_code} for {report_date}.'))
          df_obs <- suppressWarnings(parse_attempt(sch_fixed))
        }
      )
    })
  
  return(df_obs)
}



#' Extract the FFIEC codebook information for a given schedule file
#'
#' `read_ffiec_codebook()` accepts a text file containing tab-separated-value
#' bulk data provided by the FFIEC, and writes the variable names, their 
#' descriptions, and the table where they can be found to a given database.
#'
#' Most applications will not use this function as it is called by
#' `extract_ffiec_tsv()` as part of extracting the codebook and observations
#' simultaneously.
#'
#' ZIP files containing valid schedule files for 2001-present are offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) 
#' 
#' @param sch_unzipped The path to a valid FFIEC schedule data file on disk
#' @return NULL
#' @export
#' @examples
#' # The database connector only needs to be created once in any given script.
#' ffiec_db <- db_connector_sqlite('./db/ffiec.sqlite')
#' read_ffiec_codebook(ffiec_db, 'FFIEC CDR Call Schedule RCCII 06302002.txt')
read_ffiec_codebook <- function(sch_unzipped) {
  sch_file <- basename(sch_unzipped)
  sch_code <- read_ffiec_sch_code(sch_file)
  
  rlog::log_info(glue::glue('Extracting codebook...'))
  report_date <- parse_mdy_substring(sch_unzipped)
  sch_code    <- read_ffiec_sch_code(sch_unzipped)
  part_code   <- read_ffiec_part_codes(sch_unzipped)
  
  tibble::tibble(
    REPORT_DATE   = report_date,
    SCHEDULE_CODE = sch_code,
    PART_NUM      = part_code[1],
    PART_OF       = part_code[2],
    VAR_CODE      = read_ffiec_varcodes(sch_unzipped),
    VAR_DESC      = read_ffiec_vardescs(sch_unzipped)
  ) %>% dplyr::filter(stringr::str_length(VAR_DESC) != 0)
}

#' Get the components of a schedule filename in easily-referenced format.
#'
#' @param sch The name of an FFIEC tab-separated schedule file.
#' @return A named vector containing a value for `report_date`, `sch_code`,
#' `part_num` (default `1`) and `part_of` (default `1`)
#' and number of parts available for the schedule.
#' @export
#' @examples
#' > read_ffiec_filename('FFIEC CDR Call Schedule RCB 03312012(1 of 2).txt')
#'  report_date     sch_code     part_num      part_of 
#' "2012-03-31"        "RCB"          "1"          "2" 
read_ffiec_filename <- function(sch) {
  sch_info <- c(report_date = parse_mdy_substring(sch),
                sch_code    = read_ffiec_sch_code(sch))
  part_codes <- read_ffiec_part_codes(sch)
  return(c(sch_info, part_codes))
}

#' Extract the schedule part number from an FFIEC schedule filename
#'
#' If data for a schedule is issued in one part (true for most), then it will
#' be determined to be part 1 of 1.
#'
#' @param sch The name of an FFIEC schedule file, existent or not.
#' @return A numeric vector with two named values: `part_num` and `part_of`,
#' with both being `1` if no part is indicated in the filename.
#' @export
#' @examples
#' > extract_part_code('FFIEC CDR Call Schedule RCB 03312013(2 of 2).txt')
#' part_num  part_of 
#'        1        2 
read_ffiec_part_codes <- function(sch) {
  rx <- '([[:digit:]]{8})(\\(([[:digit:]]) of ([[:digit:]])\\))*'
  part_codes <- stringr::str_match(sch, rx)
  if (is.na(part_codes[1, 3])) return(c(part_num = 1, part_of = 1))
  return(c(part_num = as.numeric(part_codes[1, 4]), 
           part_of  = as.numeric(part_codes[1, 5])))
}

#' Extract the alphabetical schedule code from an FFIEC schedule filename
#'
#' @param sch The name of an FFIEC schedule file, existent or not.
#' @return A character value containing the alphabetical schedule code 
#' corresponding to the given schedule filename.
#' @export
#' @examples
#' > read_ffiec_sch_code('FFIEC CDR Call Schedule RCCII 06302002.txt')
#' [1] "RCCII"
read_ffiec_sch_code <- function(sch) {
  stringr::str_match(sch, '([[:alpha:]]+) [[:digit:]]{8}')[1, 2]
}

#' Extract the variable names from an FFIEC schedule file
#'
#' @param sch_unzipped The path to an FFIEC schedule file on disk.
#' @return A character vector containing the variable names found in the top line.
#' @export
#' @examples
#' read_ffiec_varcodes('FFIEC CDR Call Schedule RCCII 06302002.txt')
read_ffiec_varcodes <- function(sch_unzipped) { 
  readr::read_lines(sch_unzipped, n_max = 1, progress = FALSE) %>% 
    str_split1('\\t')
}

#' Extract the variable descriptions from an FFIEC schedule file
#'
#' @param sch_unzipped The path to an FFIEC schedule file on disk.
#' @return A character vector containing the variable descriptions found in the 
#' second line of the file.
#' @export
#' @examples
#' read_ffiec_vardescs('FFIEC CDR Call Schedule RCCII 06302002.txt')
read_ffiec_vardescs <- function(sch_unzipped) { 
  readr::read_lines(sch_unzipped, skip = 1, n_max = 1, progress = FALSE) %>%
    str_split1('\\t')
}


