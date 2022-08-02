library(community)

data_reformat_sdad(list.files("data/distribution", "\\.csv\\.xz$", full.names = TRUE), "docs/data")

files <- c("block_group", "tract", "county")
files <- structure(paste0(files, ".csv.xz"), names = files)
data_add(
  files[which(file.exists(paste0("docs/data/", files)))[1]],
  meta = list(
    ids = list(variable = "ID", map = "data/entity_info.json"),
    time = "time",
    variables = "data/distribution/measure_info.json"
  ),
  dir = "docs/data",
  clean = TRUE,
  refresh = TRUE
)

site_build(".", serve = TRUE, options = list(
  polygon_outline = .5, color_scale_center = "median"
))
