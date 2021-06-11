#################################################
# Parepare data - 6/10/2020
# Assign tasks to RAs. To run every week.
# Input: data_in/
# Output: data_out/tweet_label_covid19.sqlite
#################################################
rm(list=ls())

library(tidyverse)
library(lubridate)
library(gtools)
library(sodium)



#==============================
# Set some important constants
#==============================

# Set name of outpub sqlite database
DB_PATH_AND_NAME <- "data_out/db_label.sqlite"

# Path to output database
con = pool::dbPool(RSQLite::SQLite(), dbname = "data_out/tweet_label.sqlite")

# db_list_tables(con) JB NOTE: this line doesn't work for some reason?
RSQLite::dbListTables(con)


#==============================
# Task assignment
#==============================


#---------------------
# Load IDs to assign
#---------------------


id_benchmark = tbl(con, "task_assign_benchmark") %>% collect()
id_assigned = tbl(con, "task_assign_coder") %>% select(unique_ID) %>% collect() %>% distinct()
id_all = tbl(con, "tweet_text") %>% select(tweet_id_T,tweet_id_B,unique_ID) %>% #filter(tweet_id <= 50 | tweet_id > 20050) %>% collect()
  collect()


# Set to assign
id_to_assign = id_all %>% anti_join(id_benchmark) %>% anti_join(id_assigned) %>% .$unique_ID

set.seed(56)
id_to_assign = id_to_assign[sample(1:length(id_to_assign), length(id_to_assign))]
length(id_to_assign)

#-------------------
# Assignment
#-------------------

task_assign <- function(con_db, unique_ID_c, coder_id_c,condition){
  table_assign = expand_grid(unique_ID = unique_ID_c, coder_id = coder_id_c) %>%
    mutate(assign_timestamp = as.integer(Sys.time()),
           condition = condition)
  pool::dbWriteTable(con_db, "task_assign_coder", table_assign, append = TRUE)
}


## Change task assignment function
task_assign_batch <- function(id_to_assign, coder_id_c, coder_per_group, n_tweets_assign,week){
  if (sum(coder_per_group) != length(coder_id_c)){
    stop("Lengths of coder_id_c and coder_per_group do not match.")
  }
  coder_id_c_group <- list()
  
  for (i in 1:length(coder_per_group)){
    coder_id_c_group[[i]] <- head(coder_id_c, coder_per_group[i])
    coder_id_c <- tail(coder_id_c, -coder_per_group[i])
  }
  
  for (i in 1:length(coder_id_c_group)){
    coder_ids <- coder_id_c_group[[i]]
    tweet_ids <- id_to_assign[((i-1) * n_tweets_assign + 1):(i * n_tweets_assign)]
    condition <- ifelse((i+week) %% 2 == 0,'L','R')
    task_assign(con, tweet_ids, coder_ids,condition)
    
    message(paste(coder_ids, collapse = ","), " | ", ((i-1) * n_tweets_assign + 1), " ", (i * n_tweets_assign))
  }
  # remove assigned tweet_ids.
  return(tail(id_to_assign, - length(coder_per_group) * n_tweets_assign))
}


if (FALSE){
  # Week 1
  id_ra <- c(1:3)
  set.seed(3)
  id_ra <- sample(id_ra)
  id_ra
  
  
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(1, 3), n_tweets_assign = 150,week = 1)
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(3, 1), n_tweets_assign = 50,week = 1)
  
  
  # Week 2
  id_ra <- c(6,7,8)
  set.seed(3)
  id_ra <- sample(id_ra)
  id_ra
  
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(1, 3), n_tweets_assign = 150)
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(3, 1), n_tweets_assign = 50)
  
  
  
  # Week 3
  id_ra <- c(6,7,8)
  set.seed(3)
  id_ra <- sample(id_ra)
  id_ra
  
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(1, 3), n_tweets_assign = 225)
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(3, 1), n_tweets_assign = 75)
  
  
  # Week 4
  id_ra <- c(6,7,8)
  set.seed(3)
  id_ra <- sample(id_ra)
  id_ra
  
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(1, 3), n_tweets_assign = 225)
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(3, 1), n_tweets_assign = 75)
  
  
  
  # Week 5
  id_ra <- c(6,8)
  set.seed(3)
  id_ra <- sample(id_ra)
  id_ra
  
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(1, 2), n_tweets_assign = 225)
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(2, 1), n_tweets_assign = 75)
  
  
  # Week 6
  id_ra <- c(6,7,8)
  set.seed(3)
  id_ra <- sample(id_ra)
  id_ra
  
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(1, 3), n_tweets_assign = 225)
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(3, 1), n_tweets_assign = 75)
  
  
  # Week 7
  id_ra <- c(6,7)
  set.seed(3)
  id_ra <- sample(id_ra)
  id_ra
  
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(1, 2), n_tweets_assign = 225)
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(2, 1), n_tweets_assign = 75)
  
  # Week 8
  id_ra <- c(6,7,8)
  set.seed(3)
  id_ra <- sample(id_ra)
  id_ra
  
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(1, 3), n_tweets_assign = 225)
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(3, 1), n_tweets_assign = 75)
  
  # Week 9
  id_ra <- c(6, 8)
  set.seed(3)
  id_ra <- sample(id_ra)
  id_ra
  
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(1, 2), n_tweets_assign = 225)
  id_to_assign <- task_assign_batch(id_to_assign, coder_id_c = id_ra, coder_per_group = rep(2, 1), n_tweets_assign = 75)
  
}
