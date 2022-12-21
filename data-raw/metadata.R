################################
# ベンチマークを取るデータの概要
################################
library(arrow)
library(dplyr)
source(here::here("R/schema.R"))

# open_dataset("data/typeB/13_tokyo/year=2021/month=1") |>
#   write_csv_arrow(file = "data/tokyo202101_small.csv")
# open_dataset("data/typeB/13_tokyo/year=2021") |> 
#   write_csv_arrow(file = "data/tokyo2021_medium.csv")
# open_dataset(here::here("data/typeB/"),
#                schema = jartic_typeB_schema) |> 
#   filter(year == 2021) |> 
#   write_csv_arrow(file = "data/all2021_large.csv")

pins_resources_local <- 
  pins::board_folder(here::here("data-raw"))

df_valid_meta <- 
  tibble::tibble(
  type = c("Small", "Medium", "Large", "Huge"),
  area = c("東京都", "東京都", "全国", "全国"),
  period = c("1ヶ月", "1年", "1年", "4年"))

arrow_data <- 
  list(
  small =
    open_dataset(here::here("data/typeB/13_tokyo/year=2021/month=1/"),
                 schema = jartic_typeB_schema),
  medium =
    open_dataset(here::here("data/typeB/13_tokyo/year=2021/"),
                 schema = jartic_typeB_schema),
  large = 
    open_dataset(here::here("data/typeB/"),
                 schema = jartic_typeB_schema) |> 
    dplyr::filter(year == 2021),
  huge =
    open_dataset(here::here("data/typeB/"),
                 schema = jartic_typeB_schema)
) |> 
  purrr::map(
    function(.x) {
      dplyr::select(.x, !c(year, month))
    }
  )

df_valid_meta$nrow <- 
  arrow_data |> 
  purrr::map_dbl(nrow)
df_valid_meta$ncol <- 
  arrow_data |> 
  purrr::map_dbl(ncol)

df_valid_meta$csv_size <-
  fs::as_fs_bytes(
    c(fs::file_size(here::here("data/tokyo202101_small.csv")),
      fs::file_size(here::here("data/tokyo2021_medium.csv")),
      fs::fs_bytes("260.3GB"),
      fs::fs_bytes("643.2GB")
    )
  )
df_valid_meta$parquet_size <-
  fs::as_fs_bytes(
    c(fs::file_size(here::here("data/typeB/13_tokyo/year=2021/month=1/part-0.parquet")),
      fs::dir_info(here::here("data/typeB/13_tokyo/year=2021/"),
                   recurse = TRUE,
                   regexp = ".parquet") |> 
        pull(size) |> 
        sum(),
      fs::dir_info(here::here("data/typeB/"),
                   recurse = TRUE,
                   regexp = "year=2021/month=.+/.+.parquet") |> 
        pull(size) |> 
        sum(),
      fs::dir_info(here::here("data/typeB/"),
                   recurse = TRUE,
                   regexp = "year=.+/month=.+/.+.parquet") |> 
        pull(size) |> 
        sum()
    )
  )

pins_resources_local |> 
  pins::pin_write(
    df_valid_meta,
    name = "validation_metadata",
    description = "ベンチマークを取るデータの概要",
    type = "csv")
