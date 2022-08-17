

library(tidyverse)
library(rvest)

# HTML Read Function ------------------------------------------------------

basic_clean <- function(df, prefix){
  
  # Clean up Column Names
  base_nms <- c("row_num", "school", "date", "game_num", "site", "opponent", "school_w_l")
  oth <- df[1, -c(1:length(base_nms))] %>% unlist()
  oth_nms <- paste0(prefix, "_", oth) %>% janitor::make_clean_names()  
  all_nms <- c(base_nms, oth_nms)
  colnames(df) <- all_nms
  
  df %>% 
    filter(school_w_l != "") %>% 
    mutate(date = as.Date(date)) %>% 
    mutate_at(vars(all_of(c("row_num", "game_num", oth_nms))), as.numeric)
}


cfb_html_read <- function(link_to_read, max_off = 27100, prefix){
  
  offset_all <- seq(0, max_off, 100)
  
  read_fn <- function(off) {
    read_html(paste0(link_to_read,off)) %>%
      html_table() %>% 
      .[[1]] %>% 
      janitor::clean_names()
  }
  
  slow_fn <- slowly(~ read_fn(.), rate = rate_delay(2))
  slow_w_retry <- insistently( ~ slow_fn(.), 
                               rate = rate_backoff(pause_base = 0.1, pause_min = 0.005, max_times = 4))
  
  out_df <- 
    lapply(offset_all, possibly(slow_w_retry, otherwise = data.frame(x1 = "fail"))) %>% 
    bind_rows()
  
  possible_clean <- possibly(basic_clean, otherwise = data.frame())
  
  possible_clean(out_df, prefix)
}


link_fn <- function(ord_by){
  paste0("https://www.sports-reference.com/cfb/play-index/",
         "sgl_finder.cgi?request=1&match=game&year_min=2000",
         "&year_max=2022&order_by=",ord_by, "&offset=")
}


# Set up Links and Prefixes -----------------------------------------------

link_prefix <- 
  c("pass_cmp" = "passing",
    "rush_att" = "rushing",
    "tot_plays" = "tot_plays",
    "td" = "touchdown",
    "xpm" = "xpm",
    "kick_ret" = "kick_ret",
    "punt_ret" = "punt_ret",
    "tackles_loss" = "tackles_loss",
    "def_int" = "def_int",
    "fumbles_rec" = "fumbles_rec",
    "punt" = "punt",
    "first_down_pass" = "first_down_pass",
    "penalty" = "penalty",
    "fumbles_lost" = "fumbles_lost",
    "opp_pass_cmp" = "opp_pass_cmp",
    "opp_rush_att" = "opp_rush_att",
    "opp_tot_plays" = "opp_tot_plays",
    "opp_td" = "opp_td",
    "opp_xpm" = "opp_xpm",
    "opp_kick_ret" = "opp_kick_ret",
    "opp_punt_ret" = "opp_punt_ret",
    # "opp_tackles_loss" = "opp_tackles_loss",
    "opp_def_int" = "opp_def_int", 
    "opp_fumbles_rec" = "opp_fumbles_rec",
    "opp_punt" = "opp_punt",
    "opp_first_down_pass" = "opp_first_down_pass",
    "opp_penalty" = "opp_penalty",
    "opp_fumbles_lost" = "opp_fumbles_lost")

all_links <- lapply(names(link_prefix), link_fn) %>% unlist()


# Read in -----------------------------------------------------------------

library(parallel)
mc <- makeCluster(detectCores() - 1)
clusterEvalQ(mc, library(tidyverse))
clusterEvalQ(mc, library(rvest))
clusterExport(mc, ls())

#33200
(start_time <- Sys.time())
# Split up to refresh between and avoid slower run-time
data_01 <- parLapply(mc, seq_along(all_links)[1:7],
                     function(k) cfb_html_read(all_links[k], 33200, link_prefix[k]))

data_02 <- parLapply(mc, seq_along(all_links)[8:14],
                     function(k) cfb_html_read(all_links[k], 33200, link_prefix[k]))

data_03 <- parLapply(mc, seq_along(all_links)[15:21],
                     function(k) cfb_html_read(all_links[k], 33200, link_prefix[k]))

data_04 <- parLapply(mc, seq_along(all_links)[22:27],
                     function(k) cfb_html_read(all_links[k], 33200, link_prefix[k]))

(end_time <- Sys.time())

end_time - start_time

stopCluster(mc)

# Combine and output ------------------------------------------------------

data_in <- c(data_01, data_02, data_03, data_04)

data_norow <- lapply(data_in, function(x) x %>% select(-row_num))

reduce_data <- reduce(data_norow, full_join) %>% unique()

clean_data <- 
  reduce_data %>% 
  janitor::remove_empty("cols") %>% 
  arrange(date, school)

write_csv(clean_data, "inter/00_data_collected.csv")

