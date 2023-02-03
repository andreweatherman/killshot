library(ncaahoopR)
library(tidyverse)
library(arrow)
library(rvest)

pbp <- arrow::read_parquet('2023_pbp.parquet')
schedule <- arrow::read_parquet('2023_schedule.parquet')
last_date <- pbp |> 
  slice_max(date) |> 
  pluck(2) |> 
  unique()

ids_to_scrape <- schedule |> 
  filter(between(as.Date(date), as.Date(last_date), Sys.Date() - 1) & !is.na(game_id)) |> 
  pluck(1)

pbp_to_join <- map_dfr(
  .x = ids_to_scrape,
  .f = function(x) {
    tryCatch({
      pbp <- get_pbp_game(x) |> 
        mutate(game_id = as.integer(game_id))
    }, error = function(e) {
      
    }
    )
  }
)

all_pbp <- bind_rows(pbp, pbp_to_join)
write_parquet(all_pbp, '2023_pbp.parquet')

