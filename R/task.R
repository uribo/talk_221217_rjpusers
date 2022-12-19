task_gs <- function(data, dtplyr = FALSE, duckdb = FALSE) {
  if (data.table::is.data.table(data) & dtplyr == FALSE & duckdb == FALSE) {
    d <- 
      data[, .(total_traffic = sum(traffic, na.rm = TRUE)), 
           keyby = .(location_no, meshcode10km)]
  } else {
    if (duckdb == TRUE) {
     data <- 
       data |> 
        arrow::to_duckdb()
    }
    d <- 
      data |> 
      dplyr::group_by(location_no, meshcode10km) |> 
      dplyr::summarise(total_traffic = sum(traffic, na.rm = TRUE),
                .groups = "drop") |> 
      dplyr::collect()
  }
  d
}

task_join <- function(data, dtplyr = FALSE, duckdb = FALSE, is_arrow = FALSE) {
  if (data.table::is.data.table(data) & dtplyr == FALSE) {
    d <- 
      setcolorder(
        data[, list(datetime, location_no, meshcode10km, traffic)][df_mesh10km, 
             on = .(meshcode10km), 
             allow.cartesian = TRUE], 
        c(5L, 6L, 1L, 7L, 2L, 3L, 4L))
  } else if (data.table::is.data.table(data) & dtplyr == TRUE) {
    d <- 
      data |> 
      dplyr::select(datetime, location_no, meshcode10km, traffic) |> 
      dplyr::left_join(df_mesh10km,
                by = c("meshcode10km" = "meshcode10km")) |> 
      dplyr::collect()
  } else if (is_arrow == FALSE) {
    d <- 
      data |> 
      dplyr::select(datetime, location_no, meshcode10km, traffic) |> 
      dplyr::left_join(df_mesh10km,
                by = c("meshcode10km" = "meshcode10km"))
  } else {
    if (duckdb == FALSE) {
      # arrow
      d <- 
        data |>
        dplyr::select(datetime, location_no, meshcode10km, traffic) |> 
        dplyr::left_join(arrow_mesh10km,
                  by = c("meshcode10km" = "meshcode10km")) |> 
        dplyr::collect()
    } else {
      # duckdb
      d <- 
        data |> 
        arrow::to_duckdb() |> 
        dplyr::select(datetime, location_no, meshcode10km, traffic) |> 
        dplyr::left_join(df_mesh10km,
                  by = c("meshcode10km" = "meshcode10km"),
                  copy = TRUE) |> 
        dplyr::collect()
    }
  }
  d
}
