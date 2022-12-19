library(jmastats)
library(jpmesh)
library(sf)
library(mapdata)
sf_use_s2(use_s2 = FALSE)

pins_resources_local <- 
  pins::board_folder(here::here("data-raw"))

sf_mesh10km <- 
  tibble::tibble(
    meshcode10km = meshcode_set(mesh_size = 10, .raw = FALSE)) |> 
  meshcode_sf(meshcode10km, 
              .type = "standard") |> 
  st_join(maps::map("japan", 
                    plot = FALSE,
                    fill = TRUE) |> 
            st_as_sf() |>
            st_make_valid() |> 
            st_union() |> 
            st_sf(), 
          join = st_intersects, 
          left = FALSE)
# 10kmメッシュに近傍の観測地点 (block_no)
df_mesh10km <- 
  sf_mesh10km |> 
  mutate(nearest_block_no = stations[st_nearest_feature(geometry, stations), ]$block_no) |> 
  st_drop_geometry() |> 
  left_join(stations |> 
              distinct(area, station_name, block_no),
            by = c("nearest_block_no" = "block_no"))

# df_mesh10km |> 
#   pull(nearest_block_no) |> 
#   unique()

pins_resources_local |> 
  pins::pin_write(
    df_mesh10km,
    name = "mesh_jma_station",
    description = "10kmメッシュに近傍の気象庁観測地点",
    type = "csv")
