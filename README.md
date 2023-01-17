# Data Commons: Hospitals

Hospital-related measures from the [U.S. Department of Health & Human Services](https://healthdata.gov).

# Structure

This is a community data repository, created with the `community::init_repository()` function.

1. `src/retrieve.R` should download and prepare data from a public source, and output files to `data/distribution`.
2. `data/distribution/measure_info.json` should contain metadata for each of the measures in the distribution data file(s).
3. `build.R` will convert the distribution data to site-ready versions, and `site.R` specifies the interface of the repository-specific data site.
