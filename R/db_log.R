db_log.chifed_ext <- 
  function(ymd_chr, msg, con = db_connector_sqlite()(), split = TRUE) {
    tstamp <- Sys.time()
    tstamp_2822 <- anytime::rfc2822(tstamp)
    tstamp_3339 <- anytime::rfc3339(tstamp)
    tbl_name <- 'LOGS.EXT_CHIFED'
    if (!DBI::dbExistsTable(con, tbl_name)) {
      DBI::dbCreateTable(
        con, tbl_name,
        fields = c(
          REPORT_DATE = 'TEXT',
          TIMESTAMP = "TEXT",
          MESSAGE = "TEXT"
        )
      )
    }
    DBI::dbWriteTable(
      con, tbl_name, append = TRUE,
      value = tibble::tibble(
        REPORT_DATE = ymd_chr,
        TIMESTAMP = tstamp_3339,
        MESSAGE = msg
      )
    )
    if (split) cat(glue::glue('{tstamp_2822}\t{msg}'), '\n')
  }

db_log.ffiec_ext <- 
  function(fname_parts, msg, con = db_connector_sqlite()(), split = TRUE) {
    tstamp <- Sys.time()
    tstamp_2822 <- anytime::rfc2822(tstamp)
    tstamp_3339 <- anytime::rfc3339(tstamp)
    tbl_name <- 'LOGS.EXT_FFIEC'
    if (!DBI::dbExistsTable(con, tbl_name)) {
      DBI::dbCreateTable(
        con, tbl_name,
        fields = c(
          REPORT_DATE = 'TEXT',
          SCH_CODE = 'TEXT',
          PART_NUM = 'INTEGER',
          PART_OF = "INTEGER",
          TIMESTAMP = "TEXT",
          MESSAGE = "TEXT"
        )
      )
    }
    DBI::dbWriteTable(
      con, tbl_name, append = TRUE,
      value = tibble::tibble(
        REPORT_DATE = fname_parts$ymd_chr,
        SCH_CODE = fname_parts$sch_code,
        PART_NUM = fname_parts$part_num,
        PART_OF = fname_parts$part_of,
        TIMESTAMP = tstamp_3339,
        MESSAGE = msg
      )
    )
    if (split) cat(glue::glue('{tstamp_2822}\t{msg}'), '\n')
  }