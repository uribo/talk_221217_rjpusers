# パッケージ・関数の読み込み --------------------------------------------------------------
library(arrow) # 10.0.1
library(dplyr) # 1.0.10
library(data.table) # 1.14.6
library(dtplyr) # 1.2.2
library(duckdb) # 0.6.1
source(here::here("R/task.R"))

pins_resources_local <- 
  pins::board_folder(here::here("data-raw"))

# スキーマの定義 -----------------------------------------------------------------
source(here::here("R/schema.R"))

# 検証用のデータの用意 --------------------------------------------------------------
# dplyr  --- tbl_{size}
# data.table, dtplyr --- dt_{size}
# arrow - traffic_{size}
# 1. Small
tbl_small <- 
  readr::read_csv(here::here("data/tokyo202101_small.csv"),
                  col_types = "ccicciiici")
dt_small <- 
  data.table::fread(here::here("data/tokyo202101_small.csv"),
                    colClasses = c("character", "character", "integer",
                                   "character", "character", "integer",
                                   "integer", "integer", "character",
                                   "integer"))
traffic_small <-
  open_dataset(here::here("data/typeB/13_tokyo/year=2021/month=1/"),
               schema = jartic_typeB_schema)
# 2. Medium
tbl_medium <- 
  readr::read_csv(here::here("data/tokyo2021_medium.csv"),
                  col_types = "ccicciiici_")
dt_medium <- 
  data.table::fread(here::here("data/tokyo2021_medium.csv"),
                    colClasses = c("character", "character", "integer",
                                   "character", "character", "integer",
                                   "integer", "integer", "character",
                                   "integer", "NULL"))
traffic_medium <-
  open_dataset(here::here("data/typeB/13_tokyo/year=2021/"),
               schema = jartic_typeB_schema)
# 3. Large (arrowのみ)
traffic_large <- 
  open_dataset(here::here("data/typeB/"),
               schema = jartic_typeB_schema) |> 
  filter(year == 2021)
# 4. Huge (arrowのみ)
traffic_huge <- 
  open_dataset(here::here("data/typeB/"),
               schema = jartic_typeB_schema)

# 1. input ----------------------------------------------------------------
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


# 2. task: group - summarise ----------------------------------------------------
task_gs_small_res <- 
  bench::mark(
    dplyr = task_gs(tbl_small),
    data.table = task_gs(dt_small),
    dtplyr = task_gs(dt_small, dtplyr = TRUE),
    arrow = task_gs(traffic_small),
    duckdb = task_gs(traffic_small, duckdb = TRUE),
    iterations = 5,
    time_unit = "s",
    check = FALSE)
pins_resources_local |> 
  pins::pin_write(
    task_gs_small_res |> 
      dplyr::select(expression, min, median, mem_alloc, total_time),
    name = "benchmark_task_gs_small",
    description = "M1 Mac memory 64GBでsmallデータを読み込んだ結果",
    type = "csv")

task_gs_medium_res <- 
  bench::mark(
    dplyr = task_gs(tbl_medium),
    data.table = task_gs(dt_medium),
    dtplyr = task_gs(dt_medium, dtplyr = TRUE),
    arrow = task_gs(traffic_medium),
    duckdb = task_gs(traffic_medium, duckdb = TRUE),
    iterations = 1,
    time_unit = "s",
    check = FALSE)
pins_resources_local |> 
  pins::pin_write(
    task_gs_medium_res |> 
      dplyr::select(expression, min, median, mem_alloc, total_time),
    name = "benchmark_task_gs_medium",
    description = "M1 Mac memory 64GBでsmallデータを読み込んだ結果",
    type = "csv")

task_gs_all_res <-
  bench::mark(
    small = task_gs(traffic_small),
    medium = task_gs(traffic_medium),
    large = task_gs(traffic_large),
    huge = task_gs(traffic_huge),
    iterations = 3,
    time_unit = "s",
    check = FALSE)

pins_resources_local |> 
  pins::pin_write(
    task_gs_all_res |> 
      dplyr::select(expression, min, median, mem_alloc, total_time),
    name = "benchmark_task_gs_all",
    description = "M1 Mac memory 64GBで4種のサイズのデータを読み込んだ結果",
    type = "csv")

# 3. task: join -----------------------------------------------------------------
df_mesh10km <- 
  pins_resources_local |> 
  pins::pin_read("mesh_jma_station") |> 
  mutate(across(everything(), 
                as.character))

arrow_mesh10km <- 
  df_mesh10km |> 
  arrow_table(schema = mesh_jma_station_schema)

task_join_small_res <- 
  bench::mark(
    dplyr = task_join(tbl_small),
    data.table = task_join(dt_small),
    dtplyr = task_join(dt_small, dtplyr = TRUE),
    arrow = task_join(traffic_small, is_arrow = TRUE),
    duckdb = task_join(traffic_small, is_arrow = TRUE, duckdb = TRUE),
    iterations = 5,
    time_unit = "s",
    check = FALSE)
pins_resources_local |> 
  pins::pin_write(
    task_join_small_res |> 
      dplyr::select(expression, min, median, mem_alloc, total_time),
    name = "benchmark_task_join_small",
    description = "M1 Mac memory 64GBでsmallデータを読み込んだ結果",
    type = "csv")

task_join_medium_res <-
  bench::mark(
    dplyr = task_join(tbl_medium),
    data.table = task_join(dt_medium),
    dtplyr = task_join(dt_medium, dtplyr = TRUE),
    arrow = task_join(traffic_medium, is_arrow = TRUE),
    duckdb = task_join(traffic_medium, is_arrow = TRUE, duckdb = TRUE),
    iterations = 3,
    time_unit = "s",
    check = FALSE)
pins_resources_local |> 
  pins::pin_write(
    task_join_medium_res |> 
      dplyr::select(expression, min, median, mem_alloc, total_time),
    name = "benchmark_task_join_medium",
    description = "M1 Mac memory 64GBでmediumデータを読み込んだ結果",
    type = "csv")
# Abort
# task_join_all_res <-
#   bench::mark(
#     small = task_join(traffic_small, is_arrow = TRUE),
#     medium = task_join(traffic_medium, is_arrow = TRUE),
#     large = task_join(traffic_large, is_arrow = TRUE),
#     huge = task_join(traffic_huge, is_arrow = TRUE),
#     iterations = 1,
#     time_unit = "s",
#     check = FALSE)
# pins_resources_local |> 
#   pins::pin_write(
#     task_join_all_res |> 
#       dplyr::select(expression, min, median, mem_alloc, total_time),
#     name = "benchmark_task_join_all",
#     description = "M1 Mac memory 64GBで4種のサイズのデータを読み込んだ結果",
#     type = "csv")

tictoc::tic();
traffic_large |> 
  distinct(location_no, meshcode10km) |> 
  left_join(arrow_mesh10km,
             by = c("meshcode10km" = "meshcode10km")) |> 
  collect();
tictoc::toc()
#> 16.746 sec elapsed

tictoc::tic();
traffic_huge |> 
  distinct(location_no, meshcode10km) |> 
  left_join(arrow_mesh10km,
             by = c("meshcode10km" = "meshcode10km")) |> 
  collect();
tictoc::toc()
#> 67.593 sec elapsed

