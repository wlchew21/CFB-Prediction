
library(tidyverse)
library(lubridate)

data_in <- read_csv("inter/00_data_collected.csv")


# Mapping for School/Opponent -------------------------------------------------

map_school <- 
  data_in %>% 
  count(school, name = "n_school")

map_opp <- 
  data_in %>% 
  count(opponent, name = "n_opponent")

full_map <- 
  full_join(map_school, map_opp, by = c("school" = "opponent")) %>% 
  arrange(school) %>% 
  mutate_if(is.numeric, replace_na, 0)

# Look at data to see any that should be standardized
full_map %>% 
  filter(n_school == 0, n_opponent > 25)

full_map %>% 
  filter(n_opponent == 0)

# Fix a few
full_map_fix <- 
  full_map %>% 
  mutate(name_fix = 
           case_when(
             str_detect(school, "Bowling Green") ~ "Bowling Green State",
             school == "Brigham Young" ~ "BYU",
             school == "Texas Christian" ~ "TCU",
             school == "Nevada-Las Vegas" ~ "UNLV",
             
             TRUE ~ school
           ))

mapping_join <- 
  full_map_fix %>% 
  distinct(school, name_fix)

# Add in the Mapping
data_mapped <- 
  data_in %>% 
  left_join(mapping_join %>% rename(school_fix = name_fix), by = "school") %>% 
  left_join(mapping_join %>% rename(opponent_fix = name_fix), by = c("opponent" = "school")) 
  

# Split the data based on the role of "School" ----------------------------


value_cols <- 
  colnames(data_mapped %>% select(-all_of(c("school", "date", "game_num",
                                            "site", "opponent", "school_w_l",
                                            "school_fix", "opponent_fix"))))



data_neutral <- 
  data_mapped %>% 
  filter(site == "N") %>% 
# If School comes first alphabetically call it the home team
# If School comes second alphabetically call it the away team
  mutate(school_alpha_home = ifelse(school_fix < opponent_fix, 1, 0))


data_school_home <- 
  data_mapped %>% 
  filter(is.na(site)) %>% 
  bind_rows(data_neutral %>% filter(school_alpha_home == 1))

data_school_away <- 
  data_mapped %>% 
  filter(site == "@") %>% 
  bind_rows(data_neutral %>% filter(school_alpha_home == 0))

# Map out for when School is the Home Team
home_school <- 
  data_school_home %>% 
  mutate(is_neutral = ifelse((site == "N") %in% TRUE, 1, 0)) %>% 
  select(date, home_team = school_fix, game_num,
         is_neutral, away_team = opponent_fix, home_win = school_w_l,
         # Map Fields to home/away and rename
         home_pass_cmp = pass_cmp_cmp,
         home_pass_att = pass_cmp_att,
         home_pass_pct = pass_cmp_pct,
         home_pass_yds = pass_cmp_yds,
         home_pass_td = pass_cmp_td,
         home_rush_att = rush_att_att,
         home_rush_yds = rush_att_yds,
         home_rush_avg_yds = rush_att_avg,
         home_rush_td = rush_att_td,
         home_tot_plays = tot_plays_plays,
         home_tot_yds = tot_plays_yds,
         home_avg_yds = tot_plays_avg,
         home_td = td_td,
         home_pts = td_pts,
         # home_pts_diff = td_diff,
         home_xp_made = xpm_xpm,
         home_xp_att = xpm_xpa,
         home_xp_pct = xpm_xp_percent,
         home_fg_made = xpm_fgm,
         home_fg_att = xpm_fga,
         home_fg_pct = xpm_fg_percent,
         home_kick_pts = xpm_pts,
         home_kick_ret = kick_ret_ret,
         home_kick_ret_yds = kick_ret_yds,
         home_kick_ret_avg = kick_ret_avg,
         home_kick_ret_td = kick_ret_td,
         home_punt_ret = punt_ret_ret,
         home_punt_ret_yds = punt_ret_yds,
         home_punt_ret_avg = punt_ret_avg,
         home_punt_ret_td = punt_ret_td,
         home_tackle_loss = tackles_loss_loss,
         home_tackle_sack = tackles_loss_sk,
         home_def_int = def_int_int,
         home_def_int_yds = def_int_yds,
         home_def_int_td = def_int_td,
         home_fumble_rec = fumbles_rec_fr,
         home_fumble_rec_yds = fumbles_rec_yds,
         home_fumble_rec_td = fumbles_rec_td,
         home_punts = punt_punts,
         home_punt_yds = punt_yds,
         home_punt_avg_yds = punt_avg,
         home_first_down_by_pass = first_down_pass_pass,
         home_first_down_by_rush = first_down_pass_rush,
         home_first_down_by_pen = first_down_pass_pen,
         home_first_down_total = first_down_pass_tot,
         home_penalty_no = penalty_no,
         home_penalty_yds = penalty_yds,
         home_fumble_lost = fumbles_lost_fum,
         home_int_lost = fumbles_lost_int,
         home_lost_tot = fumbles_lost_tot,
         away_pass_cmp = opp_pass_cmp_cmp,
         away_pass_att = opp_pass_cmp_att,
         away_pass_pct = opp_pass_cmp_pct,
         away_pass_yds = opp_pass_cmp_yds,
         away_pass_td = opp_pass_cmp_td,
         away_rush_att = opp_rush_att_att,
         away_rush_yds = opp_rush_att_yds,
         away_rush_avg_yds = opp_rush_att_avg,
         away_rush_td = opp_rush_att_td,
         away_tot_plays = opp_tot_plays_plays,
         away_tot_yds = opp_tot_plays_yds,
         away_avg_yds = opp_tot_plays_avg,
         away_td = opp_td_td,
         away_pts = opp_td_opp,
         away_xp_made = opp_xpm_xpm,
         away_xp_att = opp_xpm_xpa,
         away_xp_pct = opp_xpm_xp_percent,
         away_fg_made = opp_xpm_fgm,
         away_fg_att = opp_xpm_fga,
         away_fg_pct = opp_xpm_fg_percent,
         away_kick_pts = opp_xpm_pts,
         away_kick_ret = opp_kick_ret_ret,
         away_kick_ret_yds = opp_kick_ret_yds,
         away_kick_ret_avg = opp_kick_ret_avg,
         away_kick_ret_td = opp_kick_ret_td,
         away_punt_ret = opp_punt_ret_ret,
         away_punt_ret_yds = opp_punt_ret_yds,
         away_punt_ret_avg = opp_punt_ret_avg,
         away_punt_ret_td = opp_punt_ret_td,
         away_def_int = opp_def_int_int,
         away_def_int_yds = opp_def_int_yds,
         away_def_int_td = opp_def_int_td,
         away_fumble_rec = opp_fumbles_rec_fr,
         away_fumble_rec_yds = opp_fumbles_rec_yds,
         away_fumble_rec_td = opp_fumbles_rec_td,
         away_punts = opp_punt_punts,
         away_punt_yds = opp_punt_yds,
         away_punt_avg_yds = opp_punt_avg,
         away_first_down_by_pass = opp_first_down_pass_pass,
         away_first_down_by_rush = opp_first_down_pass_rush,
         away_first_down_by_pen = opp_first_down_pass_pen,
         away_first_down_total = opp_first_down_pass_tot,
         away_penalty_no = opp_penalty_no,
         away_penalty_yds = opp_penalty_yds,
         away_fumble_lost = opp_fumbles_lost_fum,
         away_int_lost = opp_fumbles_lost_int,
         away_lost_tot = opp_fumbles_lost_to
         )


# Map for when School is the Away team
away_school <- 
  data_school_away %>% 
  mutate(home_win = ifelse(school_w_l == "W", "L", "W"),
         is_neutral = ifelse(site == "N", 1, 0)) %>% 
  select(date, home_team = opponent_fix, game_num,
         is_neutral, away_team = school_fix, home_win,
         # Map Fields to home/away and rename
         home_pass_cmp = opp_pass_cmp_cmp,
         home_pass_att = opp_pass_cmp_att,
         home_pass_pct = opp_pass_cmp_pct,
         home_pass_yds = opp_pass_cmp_yds,
         home_pass_td = opp_pass_cmp_td,
         home_rush_att = opp_rush_att_att,
         home_rush_yds = opp_rush_att_yds,
         home_rush_avg_yds = opp_rush_att_avg,
         home_rush_td = opp_rush_att_td,
         home_tot_plays = opp_tot_plays_plays,
         home_tot_yds = opp_tot_plays_yds,
         home_avg_yds = opp_tot_plays_avg,
         home_td = opp_td_td,
         home_pts = opp_td_opp,
         home_xp_made = opp_xpm_xpm,
         home_xp_att = opp_xpm_xpa,
         home_xp_pct = opp_xpm_xp_percent,
         home_fg_made = opp_xpm_fgm,
         home_fg_att = opp_xpm_fga,
         home_fg_pct = opp_xpm_fg_percent,
         home_kick_pts = opp_xpm_pts,
         home_kick_ret = opp_kick_ret_ret,
         home_kick_ret_yds = opp_kick_ret_yds,
         home_kick_ret_avg = opp_kick_ret_avg,
         home_kick_ret_td = opp_kick_ret_td,
         home_punt_ret = opp_punt_ret_ret,
         home_punt_ret_yds = opp_punt_ret_yds,
         home_punt_ret_avg = opp_punt_ret_avg,
         home_punt_ret_td = opp_punt_ret_td,
         home_def_int = opp_def_int_int,
         home_def_int_yds = opp_def_int_yds,
         home_def_int_td = opp_def_int_td,
         home_fumble_rec = opp_fumbles_rec_fr,
         home_fumble_rec_yds = opp_fumbles_rec_yds,
         home_fumble_rec_td = opp_fumbles_rec_td,
         home_punts = opp_punt_punts,
         home_punt_yds = opp_punt_yds,
         home_punt_avg_yds = opp_punt_avg,
         home_first_down_by_pass = opp_first_down_pass_pass,
         home_first_down_by_rush = opp_first_down_pass_rush,
         home_first_down_by_pen = opp_first_down_pass_pen,
         home_first_down_total = opp_first_down_pass_tot,
         home_penalty_no = opp_penalty_no,
         home_penalty_yds = opp_penalty_yds,
         home_fumble_lost = opp_fumbles_lost_fum,
         home_int_lost = opp_fumbles_lost_int,
         home_lost_tot = opp_fumbles_lost_to,
         away_pass_cmp = pass_cmp_cmp,
         away_pass_att = pass_cmp_att,
         away_pass_pct = pass_cmp_pct,
         away_pass_yds = pass_cmp_yds,
         away_pass_td = pass_cmp_td,
         away_rush_att = rush_att_att,
         away_rush_yds = rush_att_yds,
         away_rush_avg_yds = rush_att_avg,
         away_rush_td = rush_att_td,
         away_tot_plays = tot_plays_plays,
         away_tot_yds = tot_plays_yds,
         away_avg_yds = tot_plays_avg,
         away_td = td_td,
         away_pts = td_pts,
         # away_pts_diff = td_diff,
         away_xp_made = xpm_xpm,
         away_xp_att = xpm_xpa,
         away_xp_pct = xpm_xp_percent,
         away_fg_made = xpm_fgm,
         away_fg_att = xpm_fga,
         away_fg_pct = xpm_fg_percent,
         away_kick_pts = xpm_pts,
         away_kick_ret = kick_ret_ret,
         away_kick_ret_yds = kick_ret_yds,
         away_kick_ret_avg = kick_ret_avg,
         away_kick_ret_td = kick_ret_td,
         away_punt_ret = punt_ret_ret,
         away_punt_ret_yds = punt_ret_yds,
         away_punt_ret_avg = punt_ret_avg,
         away_punt_ret_td = punt_ret_td,
         away_tackle_loss = tackles_loss_loss,
         away_tackle_sack = tackles_loss_sk,
         away_def_int = def_int_int,
         away_def_int_yds = def_int_yds,
         away_def_int_td = def_int_td,
         away_fumble_rec = fumbles_rec_fr,
         away_fumble_rec_yds = fumbles_rec_yds,
         away_fumble_rec_td = fumbles_rec_td,
         away_punts = punt_punts,
         away_punt_yds = punt_yds,
         away_punt_avg_yds = punt_avg,
         away_first_down_by_pass = first_down_pass_pass,
         away_first_down_by_rush = first_down_pass_rush,
         away_first_down_by_pen = first_down_pass_pen,
         away_first_down_total = first_down_pass_tot,
         away_penalty_no = penalty_no,
         away_penalty_yds = penalty_yds,
         away_fumble_lost = fumbles_lost_fum,
         away_int_lost = fumbles_lost_int,
         away_lost_tot = fumbles_lost_tot
         )




# Get down to unique dataset ----------------------------------------------


game_records_w_dup <-
  home_school %>%
  bind_rows(away_school) %>% 
  mutate(season = case_when(month(date) >= 8 ~ year(date),
                            month(date) <= 3 ~ year(date) - 1))

game_records <- 
  game_records_w_dup %>%
  group_by(season, date, home_team, game_num, is_neutral, away_team, home_win) %>% 
  # Inconsistiencies in data entry - take max to assume most
  summarize_all(max) %>% 
  ungroup()


write_csv(game_records, "inter/01_game_records_data.csv")
