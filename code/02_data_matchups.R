
library(tidyverse)

game_records <- read_csv("inter/01_game_records_data.csv")

game_records %>% 
  mutate_if(is.numeric, replace_na, 0)

# Get Matchups and Teams stats --------------------------------------------

matchups <- 
  game_records %>% 
  distinct(season, date, home_team, away_team, is_neutral, home_win)

home_team_stats <- 
  game_records %>% 
  select(season, date, starts_with("home_"), -home_win) %>% 
  rename_at(vars(starts_with("home_")), function(x) str_remove(x, "home_"))

away_team_stats <- 
  game_records %>% 
  select(season, date, starts_with("away_")) %>% 
  rename_at(vars(starts_with("away_")), function(x) str_remove(x, "away_"))

team_stats <- 
  home_team_stats %>% 
  bind_rows(away_team_stats) %>% 
  arrange(season, team, date) %>% 
  group_by(season, team) %>% 
  mutate(game_num = row_number()) %>% 
  ungroup()

game_nums <- 
  team_stats %>% 
  distinct(date, team, game_num)

# Add in Game #
set_matchup <- 
  matchups %>% 
  left_join(game_nums %>% rename(home_team = team, home_game_num = game_num)) %>% 
  left_join(game_nums %>% rename(away_team = team, away_game_num = game_num))  


# Function to setuip data -------------------------------------------------


pre_matchup_data <- function(k, agg_fn = mean, agg_desc = "avg"){
  
  this_matchup <- set_matchup[k, ]
  
  home_sn_adj <- ifelse(this_matchup$home_game_num == 1, 1, 0)
  away_sn_adj <- ifelse(this_matchup$away_game_num == 1, 1, 0)
  
  home_prior_stats <- 
    team_stats %>% 
    filter(team == this_matchup$home_team, 
           season == this_matchup$season - home_sn_adj, 
           date < this_matchup$date) %>% 
    select(-season, -game_num) %>% 
    group_by(team) %>% 
    summarize_if(is.numeric, agg_fn, na.rm = TRUE) %>% 
    rename_all(~paste0("home_", agg_desc, "_",  .))
  
  away_prior_stats <- 
    team_stats %>% 
    filter(team == this_matchup$away_team, 
           season == this_matchup$season - away_sn_adj, 
           date < this_matchup$date) %>% 
    select(-season, -game_num) %>% 
    group_by(team) %>% 
    summarize_if(is.numeric, agg_fn, na.rm = TRUE) %>% 
    rename_all(~paste0("away_", agg_desc, "_", .))
 
  matchup_stats <- 
    this_matchup %>% 
    left_join(home_prior_stats, by = c("home_team" = paste0("home_", agg_desc, "_team"))) %>% 
    left_join(away_prior_stats, by = c("away_team" = paste0("away_", agg_desc, "_team")))
   
  matchup_stats
}


# Data Setup --------------------------------------------------------------

library(parallel)
mc <- makeCluster(detectCores() - 1)
clusterEvalQ(mc, library(tidyverse))
clusterExport(mc, ls())

st <- Sys.time()
matchup_data_avg <- 
  parLapply(mc, 1:nrow(set_matchup), pre_matchup_data) %>%
  bind_rows()

matchup_data_sum <- 
  parLapply(mc, 1:nrow(set_matchup), pre_matchup_data) %>%
  bind_rows()


en <- Sys.time()
en - st

stopCluster(mc)


# Output ------------------------------------------------------------------

write_csv(matchup_data_avg, "inter/02_matchup_data_average.csv")
write_csv(matchup_data_sum, "inter/02_matchup_data_sum.csv")
