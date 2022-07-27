input_dataview(id = "default_view", y = "selected_variable", time_agg = "selected_time")

page_section(
  wraps = "col", sizes = c(4, NA),
  page_section(
    wraps = "row",
    output_info(title = "variables.short_name"),
    input_button("Download", "export", query = list(include = "selected_variable"), class = "btn-full"),
    output_info(body = "variables.sources"),
    page_section(
      wraps = "col", sizes = c(NA, 1),
      input_select(
        "Variable", options = "variables", default = 3,
        id = "selected_variable"
      ),
      input_number("Time", variable = "time", default = "last", id = "selected_time")
    ),
    output_legend(id = "main_legend", subto = c("main_map", "main_plot")),
    output_info(
      default = c(body = "Hover over output elements for more information."),
      title = "features.name",
      subto = c("main_map", "main_plot", "main_legend")
    ),
    output_info(
      body = c(
        "variables.long_name" = "selected_variable",
        "variables.statement"
      ),
      row_style = c("stack", "table"),
      subto = c("main_map", "main_plot", "main_legend"),
      variable_info = FALSE
    )
  ),
  page_section(
    {
      files <- c("block_group", "tract", "county")
      files <- structure(paste0(files, ".csv.xz"), names = files)
      layers <- lapply(paste0("docs/", list.files("docs", "^points_")), function(f) list(
        url = f, time = as.numeric(gsub("[^0-9]", "", f))
      ))
      if (file.exists("docs/map_2020.geojson")) output_map(
        list(
          list(
            name = names(files[which(file.exists(paste0("docs/data/", files)))[1]]),
            time = 2020,
            url = "docs/map_2020.geojson",
            id_property = "geoid"
          )
        ),
        overlays = lapply(
          names(jsonlite::read_json("data/distribution/measure_info.json"))[-1],
          function(var) list(variable = var, source = layers)
        ),
        id = "main_map",
        subto = c("main_plot", "main_legend"),
        options = list(
          attributionControl = FALSE,
          scrollWheelZoom = FALSE,
          height = "500px",
          zoomAnimation = "settings.map_zoom_animation"
        ),
        tiles = list(
          light = list(url = "https://stamen-tiles-{s}.a.ssl.fastly.net/toner-lite/{z}/{x}/{y}{r}.png"),
          dark = list(url = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png")
        ),
        attribution = list(
          list(
            name = "Stamen toner-light",
            url = "https://stamen.com",
            description = "Light-theme map tiles by Stamen Design"
          ),
          list(
            name = "CARTO Dark Matter",
            url = "https://carto.com/attributions",
            description = "Dark-theme map tiles by CARTO"
          ),
          list(
            name = "OpenStreetMap",
            url = "https://www.openstreetmap.org/copyright"
          )
        )
      )
    },
    output_plot(
      x = "time", y = "selected_variable", id = "main_plot", subto = c("main_map", "main_legend"),
      options = list(
        layout = list(
          xaxis = list(title = FALSE, fixedrange = TRUE),
          yaxis = list(fixedrange = TRUE, zeroline = FALSE)
        ),
        data = data.frame(
          type = c("plot_type", "box"), fillcolor = c(NA, "transparent"),
          hoverinfo = c("text", NA), mode = "lines+markers", showlegend = FALSE,
          name = c(NA, "Summary"), marker.line.color = "#767676", marker.line.width = 1
        ),
        config = list(modeBarButtonsToRemove = c("select2d", "lasso2d", "sendDataToCloud"))
      )
    )
  )
)