library(bench)
pins_resources_local <- 
  pins::board_folder(here::here("data-raw"))
input_csv <-
  list(small = here::here("data/tokyo202101_small.csv"),
       medium = here::here("data/tokyo2021_medium.csv"))
df_benchmark_input_small <- 
  mark(
  `readr::read_csv` = readr::read_csv(input_csv$small),
  `data.table::fread` = data.table::fread(input_csv$small),
  `arrow::read_csv_arrow` = arrow::read_csv_arrow(input_csv$small),
  iterations = 5,
  check = FALSE,
  time_unit = "s")
df_benchmark_input_medium <-
  mark(
  `readr::read_csv` = readr::read_csv(input_csv$medium),
  `data.table::fread` = data.table::fread(input_csv$medium),
  `arrow::read_csv_arrow` = arrow::read_csv_arrow(input_csv$medium),
  iterations = 3,
  check = FALSE)

pins_resources_local |> 
  pins::pin_write(
    df_benchmark_input_small |> 
      dplyr::select(expression, min, median, mem_alloc, total_time),
    name = "benchmark_input_small",
    description = "M1 Mac memory 64GBでsmallデータを読み込んだ結果",
    type = "csv")
pins_resources_local |> 
  pins::pin_write(
    df_benchmark_input_medium |> 
      dplyr::select(expression, min, median, mem_alloc, total_time),
    name = "benchmark_input_medium",
    description = "M1 Mac memory 64GBでmediumデータを読み込んだ結果",
    type = "csv")
