library(ncaahoopR)
library(tidyverse)
library(arrow)
library(rvest)

get_all_ids <- function() {
  
  # loop through schedule
  slugs <- read_html('https://github.com/lbenz730/ncaahoopR_data/tree/master/2022-23/schedules') |> 
    html_nodes('.js-navigation-open.Link--primary') |> 
    html_text()
  
  schedules <- map_dfr(
    .x = slugs,
    .f = function(x) {
      base <- 'https://github.com/lbenz730/ncaahoopR_data/raw/master/2022-23/schedules/'
      url <- paste0(base, x)
      
      read_csv(url)
    }
  )
  
  return(schedules)

}

schedules <- read.csv('https://github.com/lbenz730/ncaahoopR_data/raw/master/2022-23/pbp_logs/schedule.csv')

last_date <- schedules |> 
  filter(!is.na(home_score)) |> 
  slice_max(date) |> 
  pluck(8) |> 
  unique()

# get games that have not been scraped
ids_to_scrape <- schedules |> 
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
  
data <- read_parquet('ncaa_pbp.parquet') |> mutate(date = as.Date(date))

all_pbp <- bind_rows(data, pbp_to_join)

