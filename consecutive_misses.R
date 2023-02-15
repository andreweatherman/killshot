## quick function to find most consecutive misses by a winning team
## using open-source ESPN play-by-play data

## this helper function to get .parquet files from a url is grabbed
## from one of Tan Ho's project's -- probably nflreadr

parquet_from_url <- function(url){
  rlang::check_installed("arrow")
  load <- try(curl::curl_fetch_memory(url), silent = TRUE)
  
  content <- try(arrow::read_parquet(load$content), silent = TRUE)
  
  data.table::setDT(content)
  return(content)
}

# load the data
## I keep an updated .parquet file on GitHub
## all scraped using `ncaahoopR` + with some new columns

data <- parquet_from_url('https://github.com/andreweatherman/killshot/raw/main/ncaa_pbp.parquet')

consecutive_misses <- function(winner_margin = 5, misses = 5) {
  
  data <- pbp |> 
    filter(shot_team == winner & free_throw == FALSE & winner_diff >= winner_margin, .by = 'game_id') |> 
    select(game_id, shot_outcome) |> 
    # make a dummy var. for shot outcome to easily total the number of misses
    # encode a miss as 1 and a make as 0; we only want to add the misses
    mutate(dummy_outcome = ifelse(shot_outcome == 'missed', 1, 0)) |> 
    # using dplyr 1.1.0, use consecutive_id to encode when there are consecutive
    # runs of the same outcome type (dummy_outcome variable)
    mutate(id = consecutive_id(dummy_outcome), .by = c(game_id)) |> 
    # total the dummy_outcome variable across each consecutive run (id) in each
    # game (game_id)
    summarize(consec_misses = sum(dummy_outcome), .by = c(game_id, id)) |> 
    # take the final row of each game -- any number > 0 means a team ended on that
    # many consecutive misses
    slice_tail(n = 1, by = game_id) |> 
    filter(consec_misses >= misses) |> 
    arrange(desc(consec_misses)) 
  
  # join back data with meta information
  results <- right_join(
    pbp |> 
      mutate(opponent = ifelse(winner == home, away, home)) |> 
      select(game_id, date,  winner, opponent, winner_diff) |> 
      # we only want one meta row for each game
      unique(),
    data |> select(-id),
    by = join_by(game_id)
  ) |> 
    select(-game_id) |> 
    setNames(c('date', 'winner', 'opponent', 'margin', 'misses')) |> 
    arrange(desc(misses))
  
  return(results)
  
}

# run it!
consecutive_misses()