################################
# Dropboxに蓄積している断面交通量情報データのファイルサイズ
################################
pins_resources_local <- 
  pins::board_folder(here::here("data-raw"))

# 201901~202210
df_my_dropbox <- 
  rdrop2::drop_dir("resources/日本道路交通情報センター/B_断面交通量情報") |> 
  dplyr::filter(.tag == "file") |> 
  dplyr::select(name, size) |> 
  ensurer::ensure(nrow(.) == 51 * 46) |> 
  dplyr::mutate(size = fs::as_fs_bytes(size))

pins_resources_local |> 
  pins::pin_write(
    df_my_dropbox,
    name = "typeB_zip_files",
    description = "Dropboxに蓄積している断面交通量情報データのファイルサイズ",
    type = "csv")
