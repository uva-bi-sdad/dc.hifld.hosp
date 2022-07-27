library(jsonlite)
library(catchment)
library(sf)
library(osrm)

dir.create("docs", FALSE)
dir.create("data/original", FALSE)
dir.create("data/original/shapes", FALSE)

# define health districts and focal counties
va_id_map <- jsonlite::read_json("https://uva-bi-sdad.github.io/community/dist/shapes/VA/virginia_2010.json")
county_districts <- c(
  unlist(lapply(va_id_map$county, "[[", "district")),
  "11001" = "11_hd_01", "24017" = "24_hd_01", "24021" = "24_hd_01",
  "24031" = "24_hd_01", "24033" = "24_hd_01"
)
states <- c("DC", "DE", "KY", "MD", "NC", "NJ", "PA", "TN", "VA", "WV")

# make a function to process each year
process_year <- function(data, dir = "data/original", districts = county_districts) {
  year <- data[1, "year"]
  message(year, " starting")
  results_file <- paste0("data/working/hhs_", year, ".csv.xz")
  block_groups <- if (file.exists(results_file)) {
    message(year, " loading existing file")
    read.csv(gzfile(results_file), check.names = FALSE)
  } else {
    #
    # get and prepare data
    #
    # get population data
    pop_file <- paste0(dir, "/population", year)
    dir.create(pop_file, FALSE)
    pop_bg_file <- "data/working/population_bg.csv.xz"
    message(year, " preparing population data")
    if (file.exists(pop_bg_file)) {
      population <- read.csv(gzfile(pop_bg_file))
    } else {
      population <- do.call(rbind, lapply(states, function(s) {
        pop <- download_census_population(pop_file, s, year)$estimates
        pop$GEOID <- as.character(pop$GEOID)
        rownames(pop) <- pop$GEOID
        shapes <- download_census_shapes("data/original/shapes", s, "bg", year = year)
        rownames(shapes) <- shapes$GEOID
        pop <- pop[shapes$GEOID,]
        pop[is.na(pop)] <- 0
        data.frame(
          GEOID = shapes$GEOID,
          state = s,
          population = pop$TOTAL.POPULATION_Total,
          population_under_18 = rowSums(
            pop[, grep("SEX\\.BY\\.AGE_[FemM]+ale_(?:U|5\\.|10|15)", colnames(pop))], na.rm = TRUE
          ),
          st_coordinates(st_centroid(shapes))
        )
      }))
      write.csv(population, xzfile(pop_bg_file), row.names = FALSE)
    }
    
    # tract-level population for comparison
    pop_tr_file <- "data/working/population_tr.csv.xz"
    if (file.exists(pop_tr_file)) {
      population_tracts <- read.csv(gzfile(pop_tr_file))
    } else {
      population_tracts <- do.call(rbind, lapply(states, function(s) {
        shapes <- download_census_shapes("data/original/shapes", s, "tr", year = year)
        rownames(shapes) <- shapes$GEOID
        pop <- population[population$state == s, ]
        do.call(rbind, lapply(split(pop, substring(pop$GEOID, 1, 11)), function(d) {
          geoid <- substring(d[1, "GEOID"], 1, 11)
          if (nrow(d) == 1) {
            d$GEOID <- geoid
            d
          } else {
            data.frame(
              GEOID = geoid,
              state = d[1, "state"],
              population = sum(d$population),
              population_under_18 = sum(d$population_under_18),
              st_coordinates(st_centroid(shapes[shapes$GEOID == geoid, ]))
            )
          }
        }))
      }))
      write.csv(population_tracts, xzfile(pop_tr_file), row.names = FALSE)
    }
    
    # aggregate provider data
    hospitals_file <- "data/working/hospitals.csv.xz"
    if (file.exists(hospitals_file)) {
      providers <- read.csv(gzfile(hospitals_file))
    } else {
      vars_const <- c("hospital_name", "hospital_subtype", "hhs_ids", "is_metro_micro", "state")
      vars_avg <- c(
        "total_beds_7_day_avg", "total_icu_beds_7_day_avg", "inpatient_beds_7_day_avg",
        "inpatient_beds_used_7_day_avg", "all_adult_hospital_beds_7_day_avg",
        "all_adult_hospital_inpatient_beds_7_day_avg", "all_adult_hospital_inpatient_bed_occupied_7_day_avg",
        "icu_beds_used_7_day_avg", "staffed_adult_icu_bed_occupancy_7_day_avg",
        "all_pediatric_inpatient_bed_occupied_7_day_avg", "all_pediatric_inpatient_beds_7_day_avg",
        "total_staffed_pediatric_icu_beds_7_day_avg"
      )
      providers <- do.call(rbind, lapply(split(data, data$hospital_pk), function(d) {
        d[d == -999999] <- NA
        d1 <- d[1,, drop = TRUE]
        coords <- as.numeric(strsplit(d1$geocoded_hospital_address, "[\\s\\(\\)]+", perl = TRUE)[[1]][-1])
        if (length(coords)) {
          data.frame(
            GEOID = d1$hospital_pk,
            X = coords[[1]],
            Y = coords[[2]],
            c(d1[vars_const], colMeans(d[, vars_avg], na.rm = TRUE))
          )
        }
      }))
      providers[is.na(providers)] <- NA
      write.csv(providers, hospitals_file, row.names = FALSE)
    }
    
    # write location files
    out <- paste0("docs/points_", year, ".geojson")
    unlink(out)
    write_sf(st_as_sf(data.frame(
      providers[, -(2:3)],
      X = round(providers$X, 6),
      Y = round(providers$Y, 6)
    ), coords = c("X", "Y")), out)
    # calculate travel times
    cost_file <- paste0("data/working/traveltimes_", year, ".csv.xz")
    if (file.exists(cost_file)) {
      message(year, " loading existing travel times")
      traveltimes <- read.csv(gzfile(cost_file), row.names = 1, check.names = FALSE)
    } else {
      message(year, " requesting travel times")
      options(osrm.server = Sys.getenv("OSRM_SERVER"))
      traveltimes <- osrmTable(
        src = population[, c("GEOID", "X", "Y")],
        dst = providers[, 1:3]
      )$duration
      if (is.null(traveltimes)) stop("failed to calculate travel times")
      write.csv(
        cbind(GEOID = rownames(traveltimes), as.data.frame(as.matrix(traveltimes))),
        xzfile(cost_file),
        row.names = FALSE
      )
    }
    # tract-level travel times for comparison
    tract_cost_file <- paste0("data/working/traveltimes_", year, "_tract.csv.xz")
    if (!file.exists(tract_cost_file)) {
      options(osrm.server = Sys.getenv("OSRM_SERVER"))
      tract_traveltimes <- osrmTable(
        src = population_tracts[, c("GEOID", "X", "Y")],
        dst = providers[, 1:3]
      )$duration
      if (is.null(tract_traveltimes)) stop("failed to calculate tract travel times")
      write.csv(
        cbind(GEOID = rownames(tract_traveltimes), as.data.frame(as.matrix(tract_traveltimes))),
        xzfile(tract_cost_file),
        row.names = FALSE
      )
    }
    #
    # calculate outputs
    #
    message(year, " calculating measures")
    # get minimum travel times
    population$hospitals_min_drivetime <- apply(traveltimes, 1, min, na.rm = TRUE)
    population$intensive_care_min_drivetime <- apply(
      traveltimes[, providers[!is.na(providers$total_icu_beds_7_day_avg) & providers$total_icu_beds_7_day_avg != 0, "GEOID"]], 1, min, na.rm = TRUE
    )
    population$childrens_hospitals_min_drivetime <- apply(
      traveltimes[, providers[providers$hospital_subtype == "Childrens Hospitals", "GEOID"]], 1, min, na.rm = TRUE
    )
    # calculate catchment ratios
    population$hospitals_per_100k <- catchment_ratio(
      population, providers, traveltimes,
      weight = "gaussian", scale = 18, normalize_weight = TRUE, return_type = 1e5,
      consumers_value = "population", providers_value = "total_beds_7_day_avg"
    )
    population$intensive_care_per_100k <- catchment_ratio(
      population, providers[!is.na(providers$total_icu_beds_7_day_avg) & providers$total_icu_beds_7_day_avg != 0,], traveltimes,
      weight = "gaussian", scale = 18, normalize_weight = TRUE, return_type = 1e5,
      consumers_value = "population", providers_value = "total_icu_beds_7_day_avg"
    )
    population$childrens_hospitals_per_100k <- catchment_ratio(
      population, providers[providers$hospital_subtype == "Childrens Hospitals",], traveltimes,
      weight = "gaussian", scale = 18, normalize_weight = TRUE, return_type = 1e5,
      consumers_value = "population_under_18", providers_value = "total_beds_7_day_avg"
    )
    population$year <- year
    ## save each year for reruns
    write.csv(population, xzfile(results_file), row.names = FALSE)
    population
  }
  block_groups <- block_groups[substring(block_groups$GEOID, 1, 5) %in% names(districts),]
  # aggregate
  agger <- function(d, part = NULL) {
    if (nrow(d)) {
      drivetimes <- grep("drivetime", colnames(d), fixed = TRUE)
      ratios <- c("hospitals_per_100k", "intensive_care_per_100k")
      total <- sum(d$population, na.rm = TRUE)
      total[total == 0] <- 1
      children_total <- sum(d$population_under_18, na.rm = TRUE)
      children_total[children_total == 0] <- 1
      res <- as.data.frame(c(
        GEOID = if (missing(part)) districts[[substring(d[1, "GEOID"], 1, 5)]] else substring(d[1, "GEOID"], 1, part),
        as.list(c(
          colMeans(d[, drivetimes, drop = FALSE], na.rm = TRUE),
          colSums(d[, ratios, drop = FALSE] * d$population, na.rm = TRUE) / total,
          childrens_hospitals_per_100k = sum(d$childrens_hospitals_per_100k * d$population_under_18) / children_total
        ))
      ))
      res
    }
  }
  message(year, " creating aggregates")
  list(
    block_groups = block_groups,
    tracts = do.call(rbind, lapply(split(block_groups, substring(block_groups$GEOID, 1, 11)), agger, 11)),
    counties = do.call(rbind, lapply(split(block_groups, substring(block_groups$GEOID, 1, 5)), agger, 5)),
    districts = do.call(rbind, lapply(split(block_groups, districts[substring(block_groups$GEOID, 1, 5)]), agger))
  )
}

# download/load latest data
# https://healthdata.gov/Hospital/COVID-19-Reported-Patient-Impact-and-Hospital-Capa/anag-cw7u
versions <- read.csv("https://healthdata.gov/resource/j4ip-wfsv.csv?$limit=99999&$select=archive_link&$order=update_date")[, 1]
link <- versions[length(versions)]
latest_file <- paste0("data/original/", basename(link))
if (!file.exists(latest_file)) download.file(link, latest_file)
fulldata <- read.csv(latest_file)
fulldata <- fulldata[fulldata$state %in% states,]
fulldata$year <- substring(fulldata$collection_week, 1, 4)

# run for each year
data <- lapply(2020, function(year) process_year(fulldata[fulldata$year == year,]))

# download and load maps
dir.create("data/original/reference_shapes", FALSE)
entity_info <- lapply(va_id_map$district, function(e) list(region_name = e$name))
for (location in c("dc", "md", "va")) {
  for (level in c("census_block_groups", "census_tracts", "counties")) {
    name <- paste0(location, "_geo_census_cb_2020_", level)
    file <- paste0(
      "data/original/reference_shapes/", location, "_",
      sub("census_", "", level, fixed = TRUE), "_2020.geojson"
    )
    if (!file.exists(file)) {
      tryCatch(download.file(paste0(
        "https://raw.githubusercontent.com/uva-bi-sdad/dc.geographies/main/data/",
        name, "/distribution/", name, ".geojson"
      ), file), error = function(e) NULL)
    }
    if (file.exists(file)) {
      d <- read_json(file)
      for (e in d$features) entity_info[[e$properties$geoid]] <- e$properties
    }
  }
}
entity_names <- unlist(lapply(entity_info, "[[", "region_name"))

# reformat and save
final <- do.call(rbind, lapply(data, function(d) {
  year = d$block_groups[1, "year"]
  d$block_groups <- d$block_groups[, colnames(d$districts)]
  d <- do.call(rbind, d)
  varnames <- colnames(d)[-1]
  d$year <- year
  d$region_type <- c(
    "5" = "county", "8" = "health district", "11" = "tract", "12" = "block group"
  )[as.character(nchar(d$GEOID))]
  rownames(d) <- d$GEOID
  d$region_name <- d$GEOID
  present_ids <- d$GEOID[d$GEOID %in% names(entity_names)]
  d[present_ids, "region_name"] <- entity_names[present_ids]
  do.call(rbind, lapply(split(d, seq_len(nrow(d))), function(r) data.frame(
    geoid = r$GEOID,
    region_type = r$region_type,
    region_name = r$region_name,
    year = year,
    measure = varnames,
    value = as.numeric(r[varnames]),
    measure_type = ifelse(grepl("drivetime", varnames, fixed = TRUE), "minutes", "per 100k")
  )))
}))

write.csv(final, xzfile("data/distribution/hhs.csv.xz"), row.names = FALSE)

# make special maps
if (!file.exists("docs/map_2020.geojson")) {
  st_write(
    rmapshaper::ms_simplify(st_read(toJSON(list(
      type = "FeatureCollection",
      crs = list(type = "name", properties = list(name = "urn:ogc:def:crs:OGC:1.3:CRS84")),
      features = unlist(lapply(
        list.files("data/original/reference_shapes", "block_groups_2020", full.names = TRUE),
        function(f) Filter(function(e) e$properties$geoid %in% final$geoid, read_json(f)$features)
      ), FALSE, FALSE)
    ), auto_unbox = TRUE), quiet = TRUE), keep_shapes = TRUE),
    "docs/map_2020.geojson"
  )
}
