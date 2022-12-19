jartic_typeB_schema = arrow::schema(
  datetime = arrow::timestamp(unit = "ms", timezone = "Asia/Tokyo"),
  source_code = arrow::utf8(),
  location_no = arrow::int32(),
  location_name = arrow::utf8(),
  meshcode10km = arrow::utf8(),
  link_type = arrow::int32(),
  link_no = arrow::int32(),
  traffic = arrow::int32(),
  to_link_end_10m = arrow::utf8(),
  link_ver = arrow::int32(),
  year = arrow::int32(),
  month = arrow::int32())

mesh_jma_station_schema <- arrow::schema(
  meshcode10km = arrow::utf8(),
  nearest_block_no = arrow::utf8(),
  area = arrow::utf8(),
  station_name = arrow::utf8())
