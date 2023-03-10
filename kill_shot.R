library(ncaahoopR)
library(tidyverse)

### kill-shot metric function replica

library(dplyr)
library(purrr)
library(readr)
library(rlang)

parquet_from_url <- function(url){
  rlang::check_installed("arrow")
  # cache_message()
  load <- try(curl::curl_fetch_memory(url), silent = TRUE)
  
  if (inherits(load, "try-error")) {
    cli::cli_warn("Failed to retrieve data from {.url {url}}")
    return(data.table::data.table())
  }
  
  content <- try(arrow::read_parquet(load$content), silent = TRUE)
  
  if (inherits(content, "try-error")) {
    cli::cli_warn("Failed to parse file with {.fun arrow::read_parquet()} from {.url {url}}")
    return(data.table::data.table())
  }
  
  data.table::setDT(content)
  return(content)
}


schedule <- toRvik::bart_season_schedule()

div_one <- left_join(schedule |> select(away) |> count(away) |> setNames(c('team', 'away')), 
            schedule |> select(home) |> count(home) |> setNames(c('team', 'home')),
            by = 'team') |> 
    group_by(team) |> 
    summarize(total = away + home) |> 
    filter(!is.na(total) & total >= 15) |> 
  pluck(1)

scoring_drought <- function(drought_length = 120, year = 2023) {
  
  all_pbp |>
    mutate(point_value = case_when(
      shot_outcome == 'made' & three_pt == TRUE ~ 3,
      shot_outcome == 'made' & free_throw == TRUE ~ 1,
      shot_outcome == 'missed' | is.na(shot_outcome) ~ 0,
      .default = 2
    ),
    opponent = ifelse(shot_team == home, away, home)) |> 
    select(game_id, date, home, away, half, secs_remaining, secs_remaining_absolute, play_length, point_value, shot_team, opponent) |> 
    mutate(id = consecutive_id(shot_team), .by = c(game_id)) |> 
    summarize(points_scored = sum(point_value), 
              time_passed = sum(play_length),
              .by = c(game_id, id, shot_team))
    
  
}


kill_shot_team <- function(team, kill_shot = 10, year = 2023, consecutive = FALSE) {
  # format year variable
  year <- paste0(year - 1, '-', year - 2000)
  
  # get team schedule from Luke
  schedule <- read.csv(paste0('https://github.com/lbenz730/ncaahoopR_data/raw/master/', year, '/pbp_logs/schedule.csv'))
  current_games <- schedule |> filter((home == team | away == team) & !is.na(home_score))
  
  # set base url to pull PBP data from Luke
  base_url <- paste0('https://github.com/lbenz730/ncaahoopR_data/raw/master/', year, '/pbp_logs/')
  
  message(paste0("Pulling PBP data for ", team, ". This will take a few seconds."))
  # pull the data
  pbp <- map2_dfr(
      .x = current_games$date,
      .y = current_games$game_id,
    .f = function(x, y) {
      url <- paste0(base_url, x, '/', y, '.csv')
      read.csv(url)
      }
  )
  
  # set filter conditions for pure runs
  if (consecutive) {
    con <- '!is.na(shot_team)'
  }
  
  else {
    con <- '!is.na(shot_team) & !point_value == 0'
  }
  
  data <- pbp |> 
    # add point value and opponent
    mutate(point_value = case_when(
             shot_outcome == 'made' & three_pt == TRUE ~ 3,
             shot_outcome == 'made' & free_throw == TRUE ~ 1,
             shot_outcome == 'missed' | is.na(shot_outcome) ~ 0,
             .default = 2
           ),
           opponent = ifelse(team == home, away, home)) |> 
    # point values of 0 need to be thrown out to calculate correct run
    filter(eval_tidy(parse_expr(con))) |> 
    select(date, opponent, shot_team, point_value) |> 
    # uh, this group_by is totally not stolen from Stack Overflow
    group_by(date, opponent, shot_team, chunk = with(rle(shot_team), rep(seq_along(lengths), lengths))) |> 
    summarize(total_points = sum(point_value), .groups = 'drop') |> 
    filter(shot_team == team & total_points >= kill_shot) |> 
    arrange(desc(total_points)) |>
    select(-chunk) |> 
    # set names for return
    setNames(c('date', 'opponent', 'team', 'points'))
    
    return(data)
  
}

kill_shot_player <- function(player = NULL, kill_shot = 10, consecutive = FALSE) {
  
  # pbp <- parquet_from_url('https://github.com/andreweatherman/killshot/raw/main/ncaa_pbp.parquet')
  # pbp <- arrow::read_parquet('ncaa_pbp.parquet')
  
  # set filter conditions for pure runs
  if (consecutive) {
    con <- '!is.na(shot_team)'
  }
  
  else {
    con <- '!is.na(shot_team) & !point_value == 0'
  }
  
  
data <- pbp |>
  # add point value and opponent
  mutate(point_value = case_when(
    shot_outcome == 'made' & three_pt == TRUE ~ 3,
    shot_outcome == 'made' & free_throw == TRUE ~ 1,
    shot_outcome == 'missed' | is.na(shot_outcome) ~ 0,
    .default = 2
  ),
  opponent = ifelse(shot_team == home, away, home)) |> 
  filter(eval_tidy(parse_expr(con))) |> 
  mutate(id = consecutive_id(shooter), .by = c(date, game_id, opponent, shot_team)) |> 
  summarise(total = sum(point_value), .by = c(date, game_id, opponent, shot_team, shooter, id)) |> 
  arrange(desc(total)) |> 
  select(-c(id, game_id)) |> 
  # set names for return
  setNames(c('date', 'opponent', 'team', 'player', 'points'))
  
  return(data)
    
  
}

kill_shot <- function(kill_shot = 10, consecutive = FALSE) {
  
  # pbp <- parquet_from_url('https://github.com/andreweatherman/killshot/raw/main/ncaa_pbp.parquet')
  pbp <- arrow::read_parquet('ncaa_pbp.parquet')
  
  # set filter conditions for pure runs
  if (consecutive) {
    con <- '!is.na(shot_team)'
  }
  
  else {
    con <- '!is.na(shot_team) & !point_value == 0'
  }
  
  data <- pbp |> 
    # add point value and opponent
    mutate(point_value = case_when(
      shot_outcome == 'made' & three_pt == TRUE ~ 3,
      shot_outcome == 'made' & free_throw == TRUE ~ 1,
      shot_outcome == 'missed' | is.na(shot_outcome) ~ 0,
      .default = 2
    ),
    opponent = ifelse(shot_team == home, away, home)) |> 
    # point values of 0 need to be thrown out to calculate correct run
    filter(eval_tidy(parse_expr(con))) |> 
    select(date, game_id, opponent, shot_team, point_value) |> 
    # uh, this group_by is totally not stolen from Stack Overflow
    group_by(date, game_id, opponent, shot_team, chunk = with(rle(shot_team), rep(seq_along(lengths), lengths))) |> 
    summarize(total_points = sum(point_value), .groups = 'drop') |> 
    filter(total_points >= kill_shot) |> 
    arrange(desc(total_points)) |>
    select(-c(chunk, game_id)) |> 
    # set names for return
    setNames(c('date', 'opponent', 'team', 'points'))
  
  return(data)
  
}

# get current games
current_games <- function(year = 2023) {
  # format year variable
  year <- paste0(year - 1, '-', year - 2000)
  # get games
  schedule <- read.csv(paste0('https://github.com/lbenz730/ncaahoopR_data/raw/master/', year, '/pbp_logs/schedule.csv'))
  current_games <- schedule |> filter(!is.na(home_score))
  return(current_games)
}


# get all pbp data
pbp_data <- function(year = 2023, start = NULL, end = NULL) {
  
  # format year variable
  year <- paste0(year - 1, '-', year - 2000)
  
  base_url <- paste0('https://github.com/lbenz730/ncaahoopR_data/raw/master/', year, '/pbp_logs/')
  
  schedule <- current_games()
  pbp <- map2_dfr(
    .x = schedule$date,
    .y = schedule$game_id,
    .f = function(x, y) {
      tryCatch({
        url <- paste0(base_url, x, '/', y, '.csv')
        read.csv(url)
        },
        error = function(e) {
          
        })

      }
  )
  
  return(pbp)
}
