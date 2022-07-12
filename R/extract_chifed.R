#' List ZIP files containing Call Report data from the Chicago Fed
#'
#' `list_chifed_zips()` expects the path of a folder containing the bulk data
#' files for 1976-2000 offered at the [Chicago Fed's website](https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data-complete-1976-2000)
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param chifed_zip_path A single character value containing an existing folder
#' containing the ZIP files
#' SQLite DB? This is meaningful only for SQLite databases.
#' @return A character vector containing the full paths of valid ZIP files
#' @export
#' @examples
#' extract_chifed_zips("./zips-chifed")
extract_chifed_zips <-
  function(chifed_zip_path = get_chifed_zip_dir()) {
    db_connector <- db_connector_sqlite(get_sqlite_file())
    closeAllConnections()
    extract_chifed_mdrm(glue::glue("{chifed_zip_path}/MDRM.zip"))
    purrr::walk(
      list_chifed_zips(chifed_zip_path),
      ~ extract_chifed_zip(.)
    )
  }

#' Extract the Chicago Fed's data observations to a database
#'
#' `extract_chifed_observations()` reads a ZIP file containing a valid XPT file.
#'
#' ZIP files containing the data are available at the [Chicago Fed](https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data-complete-1976-2000).
#' They should be saved (but not extracted) to an empty folder.
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param zip_file An existing ZIP file containing a valid XPT data file
#' @export
#' @examples
#' chifed_db <- sqlite_connector("./db/chifed.sqlite")
#' extract_chifed_zip(chifed_db, "./zips-chifed/call0003-zip.zip")
extract_chifed_zip <- function(zip_file = NULL) {
  if (is.null(zip_file)) {
    zip_file <- rstudioapi::selectFile(path = get_chifed_zip_dir())
  }
  yy <- stringr::str_match(zip_file, "([[:digit:]]{2})[[:digit:]]{2}")[1, 2]
  yyyy <- ifelse(yy == "00", "2000", paste0("19", yy))
  mm <- stringr::str_match(zip_file, "[[:digit:]]{2}([[:digit:]]{2})")[1, 2]
  ymd_chr <- as.character(qtr_end(glue::glue("{yyyy}-{mm}-01")))

  xpt_file <-
    unzip(zip_file, list = TRUE)[["Name"]] %>%
    subset(stringr::str_detect(., "\\.(XPT|xpt)"))

  zip_base <- basename(zip_file)
  db_log.chifed_ext(
    ymd_chr,
    glue::glue("Reading records from {zip_base} > {xpt_file}")
  )
  df_wide <- unz(zip_file, xpt_file) %>% haven::read_xpt()
  db_log.chifed_ext(ymd_chr, paste(
    "Extracted", nrow(df_wide),
    "rows with", ncol(df_wide),
    "columns from", xpt_file
  ))
  
  df_wide %<>%
    dplyr::mutate(
      IDRSSD = as.character(RSSD9001),
      QUARTER_ID = ymd_chr_to_qtr_id(ymd_chr)
    ) %>%
    dplyr::select(
      IDRSSD, QUARTER_ID,
      !any_of(c("RSSD9999", "RSSD9001", "ENTITY", "DATE"))
    )

  xpt_varcodes <- names(df_wide)
  new_varcodes <- xpt_varcodes %>% subset(. %not_in% c("IDRSSD", "RSSD9999"))
  write_varcodes(new_varcodes)
  
  db_log.chifed_ext(ymd_chr, "Eliminating variables with no observations...")
  df_wide %<>% dplyr::select_if(~ !all(is.na(.)))
  db_log.chifed_ext(ymd_chr, glue::glue(
    '{ncol(df_wide)} columns with valid observations remain in the dataset'
  ))
  
  subset_chifed_vars(df_wide, '^TE') %>%
    write_chifed_observations(ymd_chr, 'TEXT')
  df_wide %<>% subset_chifed_vars('^TE', negate = TRUE)
  
  subset_chifed_vars(df_wide, '^RSSD') %>%
    write_chifed_observations(ymd_chr, 'RSSD')
  df_wide %<>% subset_chifed_vars('^RSSD', negate = TRUE)
  
  subset_chifed_vars(df_wide, '^RCF[DFW]') %>%
    write_chifed_observations(ymd_chr, 'RCFD')
  df_wide %<>% subset_chifed_vars('^RCF[DFW]', negate = TRUE)
  
  subset_chifed_vars(df_wide, '^RCFN') %>%
    write_chifed_observations(ymd_chr, 'RCFN')
  df_wide %<>% subset_chifed_vars('^RCFN', negate = TRUE)
  
  subset_chifed_vars(df_wide, '^RC(O[NFW]|F[[:digit:]])') %>%
    write_chifed_observations(ymd_chr, 'RCON')
  df_wide %<>% subset_chifed_vars('^RC(O[NFW]|F[[:digit:]])', negate = TRUE)
  
  subset_chifed_vars(df_wide, '^RCOS') %>%
    write_chifed_observations(ymd_chr, 'RCOS')
  df_wide %<>% subset_chifed_vars('^RCOS', negate = TRUE)
  
  subset_chifed_vars(df_wide, '^RIAD') %>%
    write_chifed_observations(ymd_chr, 'RIAD')
  df_wide %<>% subset_chifed_vars('^RIAD', negate = TRUE)
  
  subset_chifed_vars(df_wide, '^RIAS') %>%
    write_chifed_observations(ymd_chr, 'RIAS')
  df_wide %<>% subset_chifed_vars('^RIAS', negate = TRUE)
  
  df_wide %>% write_chifed_observations(ymd_chr, 'OTHER')
  
  db_log.chifed_ext(ymd_chr, glue::glue(
    "Successfully extracted {zip_base} > {xpt_file} to the database!"
  ))
  cat(paste(rep('-', 80), collapse = ''))
  cat("\n")
}

#' Subset a wide-format Chicago Fed data extract by variable name pattern
#' 
#' Because there are so many variables in the Chicago Fed data set, there are
#' millions of rows in the resulting data set after pivoting into long form.
#' Splitting the data up by a variable name pattern allows us to store variables
#' with similar names in their own table, reducing the length of table scans
#' during queries and speeding up return of results.
#'
#' For matching *ALL* members of a variable group like "RSSD", "RCON", "RCFD", 
#' etc., it is best to match on the first two letters because of how the data
#' is disaggregated, with each layer of reporting being given a separate 6-digit
#' suffix on a  two-letter prefix instead of the standard 4-letter, 4-digit
#' variable code.
#'
#' @param df_wide A data frame extracted from a Chicago Fed data set
#' @param pattern A pattern to match variable names on
#' @param negate (default `FALSE`) Negate the match given by `pattern`?
#' @return A `tibble` with the requested variables along with the necessary
#' identifying columns
#' @export
#'
#' @examples
#' df_rcon <- subset_chifed_vars(df_wide, '^RC.{6}') ## Subset to RCON variables
#' df_text <- subset_chifed_vars(df_wide, '^TE.{6}') ## Subset to TEXT variables
subset_chifed_vars <- function(df_wide, pattern, negate = FALSE) {
  varcodes <- 
    names(df_wide) %>%
    subset(. %not_in% c('IDRSSD', 'QUARTER_ID', 'CALL8786', 'CALL8787'))
  if (length(varcodes) == 0) return(NULL)
  
  vars_matched <- ifelse(
    negate,
    subset(varcodes, !grepl(pattern, varcodes)),
    subset(varcodes,  grepl(pattern, varcodes))
  )
  if (length(vars_matched) == 0 | is.na(vars_matched)) return(NULL)
  
  if (negate) {
    return(
      dplyr::select(
        df_wide,
        all_of(c('IDRSSD', 'QUARTER_ID')),
        any_of(c('CALL8786', 'CALL8787')),      
        !tidyselect::matches(pattern)
      )
    )
  }
  
  dplyr::select(
    df_wide,
    all_of(c('IDRSSD', 'QUARTER_ID')),
    any_of(c('CALL8786', 'CALL8787')),      
    tidyselect::matches(pattern)
  )
}

#' Write a dataframe of Chicago Fed observations to the database
#'
#' @param db_connector A `function` created by one of the `db_connector_*()`
#' functions found in this package. It should be passed without the `()`
#' @param df_long A dataframe of observations in long form
#' @export
write_chifed_observations <- function(df_wide, ymd_chr, suffix) {
  if (is.null(df_wide)) return()
  tbl_name <- glue::glue("CHIFED.OBS_{suffix}")
  df_wide %<>% dplyr::select(!tidyselect::any_of(c('RSSD9999', 'DATE_SAS')))
  if ("CALL8787" %not_in% names(df_wide)) {
    df_wide %<>% dplyr::mutate(CALL8787 = NA)
  }
  
  db_log.chifed_ext(
    ymd_chr, 
    paste(
      "Pivoting", nrow(df_wide), "observations of", 
      ncol(df_wide) - 4, "variables into long form..."
    )
  )
  
  df_long <- 
    tidyr::pivot_longer(
      df_wide,
      cols = 
        !tidyselect::any_of(c("IDRSSD", "QUARTER_ID", "CALL8786", "CALL8787")),
      names_to = "VARCODE",
      values_to = "VALUE",
      values_transform = as.character,
      values_drop_na = TRUE
    ) %>%
    dplyr::inner_join(fetch_varcodes(), by = "VARCODE") %>%
    dplyr::rename(VARCODE_ID = ID) %>%
    dplyr::select(IDRSSD, QUARTER_ID, CALL8786, CALL8787, VARCODE_ID, VALUE)
  
  db_log.chifed_ext(ymd_chr, paste0(
    "Writing ", nrow(df_long), " non-NA observations of ",
    length(unique(df_long$VARCODE_ID)), " variables to \"", tbl_name, "\"..."
  ))
  
  db_connector <- db_connector_sqlite()
  db_conn <- db_connector()
  
  tryCatch(
    {
      DBI::dbBegin(db_conn)
      if (!DBI::dbExistsTable(db_conn, tbl_name)) {
        DBI::dbCreateTable(db_conn, tbl_name,
                           fields = c(
                             IDRSSD = "INTEGER",
                             QUARTER_ID = "INTEGER",
                             CALL8786 = "INTEGER",
                             CALL8787 = "INTEGER",
                             VARCODE_ID = "INTEGER",
                             VALUE = "TEXT"
                           )
        )
      }
      DBI::dbWriteTable(db_conn, tbl_name, df_long, append = TRUE)
      DBI::dbCommit(db_conn)
    },
    warning = stop,
    error = stop
  )
  
  DBI::dbDisconnect(db_conn)
}

#' Extract the Chicago Fed's codebook data to a database
#'
#' `extract_chifed_mdrm()` extracts the Chicago Fed's call report data
#' dictionary provided at (their website)[https://www.federalreserve.gov/apps/mdrm/download_mdrm.htm]
#' to the SQLite file specified in `set_sqlite_file()`.
#'
#' @param codebook_zip An existing copy of `MDRM.zip`
#' page accompanying the main data.
#' @return NULL
#' @export
#' @examples
#' extract_chifed_mdrm("./MDRM.zip")
extract_chifed_mdrm <- function(codebook_zip) {
  mdrm <-
    unz(codebook_zip, "MDRM_CSV.csv") %>%
    readr::read_csv(
      skip = 1,
      col_types = readr::cols(.default = "c"),
      name_repair = repair_colnames,
      progress = FALSE
    ) %>%
    dplyr::select(-SERIESGLOSSARY, -UNNAMED_11) %>%
    dplyr::mutate(VARCODE = paste0(MNEMONIC, ITEM_CODE)) %>%
    dplyr::select(VARCODE, everything())
  db_connector <- db_connector_sqlite()
  db_conn <- db_connector()
  try(DBI::dbWriteTable(db_conn, "CHIFED.CODEBOOK", mdrm, overwrite = TRUE))
  DBI::dbDisconnect(db_conn)
}
