#' Extract an entire directory of FFIEC ZIP files to a database
#'
#' `extract_ffiec_zips()` examines all ZIP files in `ffiec_zip_path` and 
#' attempts to extract each enclosed schedule file to the database set by the
#' user. The underlying extraction function skips schedule files that have
#' already been successfully extracted to the database.
#'
#' @param ffiec_zip_path (default `get_ffiec_zip_dir()`) A single character
#' value containing an existing folder containing the ZIP files
#' @export
extract_ffiec_zips <- 
  function(ffiec_zip_path = get_ffiec_zip_dir()) {
    old_vroom_progress_option <- Sys.getenv('VROOM_SHOW_PROGRESS')
    Sys.setenv(VROOM_SHOW_PROGRESS = 'false')
    closeAllConnections()
    capture.output(
      purrr::pwalk(
        list_ffiec_zips_and_tsvs(ffiec_zip_path), 
        ~ extract_ffiec_tsv(..1, ..2)
      ),
      file = generate_log_name("extraction_ffiec"),
      split = TRUE
    )
    Sys.setenv(VROOM_SHOW_PROGRESS = old_vroom_progress_option)
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
#' ffiec_db <- sqlite_connector("./db/ffiec.sqlite")
#' extract_ffiec_tsv(
#'   ffiec_db,
#'   "./zips-ffiec/FFIEC FFIEC CDR Call Bulk All Schedules 03312018.zip",
#'   "FFIEC CDR Call Schedule RCCII 06302002.txt"
#' )
extract_ffiec_tsv <- 
  function(zf, sch) {
    fname_parts <- split_ffiec_sch_name(sch)
    
    date_sch <- glue::glue("{fname_parts$ymd_chr} {fname_parts$sch_code}")
    part_str <- glue::glue("({fname_parts$part_num} of {fname_parts$part_of})")
    if (part_str == "(1 of 1)") part_str <- ""
    
    rlog::log_info(glue::glue("{date_sch} {part_str}"))
    unzip(zf, sch, exdir = tempdir(), unzip = getOption("unzip"))
    tsv_tmp <- file.path(tempdir(), sch)
    
    rlog::log_info(glue::glue("Reading observations from schedule..."))
    parsed_data <- parse_ffiec_tsv(tsv_tmp)
    df_codebook <- parsed_data$codebook
    df_wide <- parsed_data$observations
    df_problems <- parsed_data$problems
    rlog::log_info(glue::glue(
      "Read {nrow(df_wide)} rows containing {ncol(df_wide)} columns"
    ))
    
    db_connector <- db_connector_sqlite()
    db_conn <- db_connector()
    DBI::dbWithTransaction(
      db_conn,
      {
        if (nrow(df_problems) > 0) {
          rlog::log_info(glue::glue(
            'Writing details on {nrow(df_problems)} parsing ', 
            'problems to "FFIEC.PROBLEMS"'
          ))
          DBI::dbWriteTable(
            db_conn, "FFIEC.PROBLEMS", df_problems, append = TRUE
          )
        }
        if (nrow(df_wide) > 0 & ncol(df_wide) > 1) {
          names(df_wide) %>%
            subset(. %not_in% c("IDRSSD", "RCON9999")) %>%
            subset(!stringr::str_detect(., "^UNNAMED_[[:digit:]]+$")) %>%
            write_varcodes(db_conn)

          rlog::log_info("Pivoting to long form...")
          df_long <-
            tidyr::pivot_longer(
              df_wide,
              cols = !tidyselect::any_of("IDRSSD"),
              names_to = "VARCODE",
              values_to = "VALUE",
              values_drop_na = TRUE
            ) %>%
            dplyr::inner_join(fetch_varcodes(db_conn), by = "VARCODE") %>%
            dplyr::mutate(
              QUARTER_ID = date_str_to_qtr_id(fname_parts$ymd_chr)
            ) %>%
            dplyr::rename(VARCODE_ID = ID) %>%
            dplyr::select(QUARTER_ID, VARCODE_ID, VARCODE, VALUE)
          
          if (nrow(df_long) == 0) {
            rlog::log_info('No non-NA observations to write. Moving on...')
            DBI::dbDisconnect(db_conn)
            cat(paste0(rep('-', 80), collapse = ''), '\n')
            unlink(c(tsv_tmp))
            return()
          }
          
          rlog::log_info(glue::glue(
            'Writing {nrow(df_codebook)} variable codes ',
            'and descriptions to "FFIEC.CODEBOOK"'
          ))
          DBI::dbWriteTable(
            db_conn, "FFIEC.CODEBOOK", df_codebook, append = TRUE
          )
          
          tbl_name <- glue::glue("FFIEC.OBS_{fname_parts$sch_code}")
          rlog::log_info(glue::glue(
            'Writing {nrow(df_long)} non-NA observations to "{tbl_name}"...'
          ))
          DBI::dbWriteTable(db_conn, tbl_name, df_long, append = TRUE)
        }
      }
    )
    DBI::dbDisconnect(db_conn)
    cat(paste0(rep('-', 80), collapse = ''), '\n')
    unlink(c(tsv_tmp))
  }


#' Parse an FFIEC TSV file into codebook, observation, and problem data frames.
#'
#' `parse_ffiec_tsv()` accepts a text file containing tab-separated-value
#' bulk data provided by the FFIEC, and writes it to a given database.
#'
#' ZIP files containing valid schedule files for 2001-present are offered at the
#' [FFIEC Bulk Data Download Service](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx)
#'
#' @param tsv The path to an unzipped FFIEC TSV schedule file
#' @return A named `list` with three `tibble` elements: `codebook`, `observations`,
#' and `problems`
#' @export
#' @examples
#' parse_ffiec_tsv("FFIEC CDR Call Schedule RCCII 06302002.txt")
parse_ffiec_tsv <- function(tsv) {
  fname_parts <- split_ffiec_sch_name(tsv)

  line_varcode <- vroom::vroom_lines(tsv, n_max = 1)
  line_vardesc <- vroom::vroom_lines(tsv, n_max = 1, skip = 1)
  lines_allobs <- vroom::vroom_lines(tsv, skip = 2)
  lines_nodesc <- c(line_varcode, lines_allobs)
  tsv_nodesc <- file.path(tempdir(), glue::glue("{fname_parts$sch_code}.nodesc"))
  vroom::vroom_write_lines(lines_nodesc, tsv_nodesc)

  # Using the extensive arguments available for `vroom::vroom()` to repair
  # column names and choose variables at read time appears to eliminate
  # the `problems` attribute attached to the resulting `tibble`. Instead, here
  # we read the data using only the `NA` detection, then extract the problems,
  # and only then do we repair the column names.
  df_wide <- 
    vroom::vroom(
      tsv_nodesc,
      na = c("", "NA", "NR", "CONF"),
      col_types = vroom::cols(.default = vroom::col_character()),
      show_col_types = FALSE,
      progress = FALSE,
      .name_repair = ~ repair_colnames(.)
    )
  
  # If there were problems parsing the data, `df_probs` will have rows. Append
  # information about the schedule file to each row for easier analysis later.
  df_probs <- vroom::problems(df_wide)
  if (nrow(df_probs) > 0) {
    df_probs %<>%
      dplyr::mutate(
        REPORT_DATE = fname_parts$ymd_chr,
        SCHEDULE_CODE = fname_parts$sch_code,
        PART_NUM = fname_parts$part_num,
        PART_OF = fname_parts$part_of,
        LINE_NUMBER = row + 1, # Because the original has a description row
        ZIP_FILE = generate_ffiec_zip_name(fname_parts$ymd_chr),
        TSV_FILE = generate_ffiec_sch_name(
          fname_parts$ymd_chr, fname_parts$sch_code,
          fname_parts$part_num, fname_parts$part_of
        )
      ) %>%
      dplyr::rename(COLUMN = col, EXPECTED = expected, ACTUAL = actual) %>%
      dplyr::select(
        REPORT_DATE, SCHEDULE_CODE, PART_NUM, PART_OF,
        LINE_NUMBER, COLUMN, EXPECTED, ACTUAL,
        ZIP_FILE, TSV_FILE
      )
  }

  df_wide %<>%
    magrittr::set_colnames(repair_colnames(names(.))) %>%
    dplyr::select(
      !tidyselect::matches("^_[[:digit:]]+$"),
      !tidyselect::any_of("RCON9999")
    )

  df_codebook <-
    tibble::tibble(
      REPORT_DATE = fname_parts$ymd_chr,
      SCHEDULE_CODE = fname_parts$sch_code,
      PART_NUM = fname_parts$part_num,
      PART_OF = fname_parts$part_of,
      VARCODE = str_split1(line_varcode, "\\t"),
      DESCRIPTION = str_split1(line_vardesc, "\\t")
    ) %>%
    dplyr::filter(VARCODE %in% names(df_wide))

  list(
    codebook = df_codebook,
    observations = df_wide,
    problems = df_probs
  )
}

#' Parse the FFIEC TSV file given by relevant parameters rather than filenames
#'
#' @param ymd_chr A reporting date
#' @param sch_code A schedule code
#' @param part_num (default `1`) A part number, if applicable
#' @param part_code (default `2`) The number of parts to the schedule, if applicable
#' @export
parse_ffiec_tsv.by_params <-
  function(ymd_chr, sch_code, part_num = 1, part_code = 1) {
    zf <- generate_ffiec_zip_name(ymd_chr)
    sch <- generate_ffiec_sch_name(ymd_chr, sch_code, part_num, part_code)
    parse_ffiec_tsv.new(zf, sch)
  }

#' Generate the path to an FFIEC ZIP filename from a reporting date
#'
#' @param ymd_chr A character reporting date in `YYYY-MM-DD` format.
#' @param full_path (default `TRUE`) Return the full path to the ZIP file?
#' @return The full path to the FFIEC ZIP filename for the given date
#' @export
#' @examples
#' generate_zip_path("2016-03-31")
generate_ffiec_zip_name <- function(ymd_chr, full_path = TRUE) {
  date_obj <- lubridate::ymd(ymd_chr)
  yyyy <- lubridate::year(ymd_chr)
  mm <- stringr::str_pad(lubridate::month(ymd_chr), 2, "left", "0")
  dd <- stringr::str_pad(lubridate::day(ymd_chr), 2, "left", "0")
  zip_name <- glue::glue("FFIEC CDR Call Bulk All Schedules {mm}{dd}{yyyy}.zip")
  if (!full_path) {
    return(zip_name)
  }
  return(file.path(get_ffiec_zip_dir(), zip_name))
}

#' Generate an FFIEC TSV schedule from a reporting date, schedule code, and
#' (for applicable schedules) part numbers.
#'
#' @param ymd_chr A character reporting date in `YYYY-MM-DD` format.
#' @param sch_code A character schedule code
#' @param part_num (default `1`) Number of the applicable schedule part
#' @param part_of (default `1`) An integer number of parts to the schedule
#' @return A character-valued ZIP filename for the given date
#' @export
#' @examples
#' generate_sch_name("2016-03-31", "RC")
#' generate_sch_name("2016-03-31", "RCO")
generate_ffiec_sch_name <-
  function(ymd_chr, sch_code, part_num = 1, part_of = 1) {
    date_obj <- lubridate::ymd(ymd_chr)
    yyyy <- lubridate::year(ymd_chr)
    mm <- stringr::str_pad(lubridate::month(ymd_chr), 2, "left", "0")
    dd <- stringr::str_pad(lubridate::day(ymd_chr), 2, "left", "0")
    parts_chr <- ifelse(part_of == 1, "", glue::glue("({part_num} of {part_of})"))
    glue::glue("FFIEC CDR Call Schedule {sch_code} {mm}{dd}{yyyy}{parts_chr}.txt")
  }

#' Split an FFIEC TSV schedule filename into its relevant constituent parts
#'
#' This function takes a file in the `glue` format: `FFIEC CDR Call Schedule {sch_code} {mm}{dd}{yyyy}{parts_chr}.txt`,
#' where `parts_chr` = `({part_num} of {part_of})` if applicable, and returns
#' a named list containing the report date in ISO `YYYY-MM-DD` format, the
#' schedule code, and (if applicable) the part number and number of parts.
#'
#' @param tsv_filename A filename matching the format of TSV files found within
#' FFIEC ZIPs.
#' @return A named `list` with, at minimum `ymd_chr` and `sch_code` representing the reporting
#' date and schedule code, respectively, and (if applicable) `part_num` and `part_of`.
#' @export
#'
#' @examples
#' sch_filename <- generate_ffiec_sch_name("2012-06-30", "RCO", 1, 2)
#' split_ffiec_sch_name(sch_filename)
split_ffiec_sch_name <- function(tsv_filename) {
  rx <- "([[:alpha:]]+) ([[:digit:]]{8})(\\(([[:digit:]]+) of ([[:digit:]]+)\\))*"
  str_parts <- stringr::str_match(tsv_filename, rx)
  fname_parts <- list(
    ymd_chr  = as.character(lubridate::mdy(str_parts[, 3])),
    sch_code = str_parts[, 2],
    part_num = ifelse(is.na(str_parts[, 4]), 1, str_parts[, 5]),
    part_of  = ifelse(is.na(str_parts[, 4]), 1, str_parts[, 6])
  )
  return(fname_parts)
}
