#################################################
# Parepare data - 5/30/2020
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

# Number of tweets to code as benchmark
N_SAMPLE_BENCHMARK = 200

# Set name of outpub sqlite database
DB_PATH_AND_NAME <- "data_out/tweet_label.sqlite"

# Path to output database
con = pool::dbPool(RSQLite::SQLite(), dbname = "data_out/tweet_label.sqlite")


#========================
# Import codebook
#========================

d_codebook <- readxl::read_excel("data_in/codebook/codebook.xlsx", sheet = 'right_task', col_types = "text")

# d_codebook <- d_codebook %>%
#   select(category, label, Description)

# SQL --------------

pool::dbExecute(con, "DROP TABLE IF EXISTS codebook;")

pool::dbExecute(con,
                "
                CREATE TABLE codebook(
                  codebook_id INTEGER PRIMARY KEY AUTOINCREMENT,
                  category TEXT,
                  label TEXT,
                  Description TEXT,
                  Condition TEXT
                )
                ")

pool::dbWriteTable(con, "codebook", d_codebook, append = TRUE)

#========================
# Import coders' credentials
#========================

d_coder_credentials <- read_csv("data_in/coder_credentials/coder_credentials.csv",
                                col_types = cols(coder_id = col_integer(),
                                                 username = col_character(),
                                                 screename = col_character(), 
                                                 passwd = col_character(),
                                                 permission = col_character()))

# Hash password for secured saving
d_coder_credentials$passwd <- sapply(d_coder_credentials$passwd, password_store)

d_coder_credentials


# SQL --------------

pool::dbExecute(con, "DROP TABLE IF EXISTS coder;")
pool::dbExecute(con,
                "
                CREATE TABLE coder(
                  coder_id INTEGER,
                  username TEXT,
                  screename TEXT,
                  passwd TEXT,
                  permission TEXT
                )
                ")
pool::dbWriteTable(con, "coder", d_coder_credentials, append = TRUE)



#========================
# Import tweet text
#========================

# JB NOTE: Need to update this with YT videos
# d_tweet_text <- readxl::read_excel("data_in/tweet_text/tweets_to_label_61k_20201105.xlsx")

d_tweet_text <- as_tibble(jsonlite:::stream_in(file('./data_in/tweet_text/data_to_label.json'))) %>%
  filter(!is.na(label))

tmp <- NULL
for(i in 1:nrow(d_tweet_text)) {
  tmpDat <- d_tweet_text[i,] %>%
    select(tweet_id_T = video_id,
           ideoT = label)
  
  tmp <- bind_rows(tmp,
                   bind_cols(tmpDat,
                             d_tweet_text %>%
                               filter(abs(label - tmpDat$ideoT) > .5) %>%
                               slice(sample(nrow(.),1)) %>%
                               select(tweet_id_B = video_id,
                                      ideoB = label)))
}

d_tweet_text <- tmp %>%
  select(tweet_id_T,tweet_id_B) %>%
  mutate(unique_ID = row_number())

# d_tweet_text <- d_tweet_text %>%
#   mutate(tweet_id = video_id,
#          tweet_text = paste0('Channel: ',channel_title,'\n',
#                              'Video Title: ',video_title,'\n',
#                              'Video Description: ',video_description),
#          quoted__tweet_id = '',
#          quoted__text = '') %>%
#   select(tweet_id,tweet_text,quoted__tweet_id,quoted__text)



summary(d_tweet_text)

names(d_tweet_text)



# SQL --------------

pool::dbExecute(con, "DROP TABLE IF EXISTS tweet_text;")

pool::dbExecute(con, 
                "
                CREATE TABLE tweet_text(
                  tweet_id_T TEXT NOT NULL,
                  tweet_id_B TEXT NOT NULL,
                  unique_ID TEXT NOT NULL UNIQUE
                )
                ")


# Add new text
pool::dbWriteTable(con, "tweet_text", as.data.frame(d_tweet_text), append = TRUE)




########################################################################################################


#================================
# Set aside a gold standard set
#================================

# Sample a benchmark set
set.seed(100)
d_assign_benchmark <- tbl(con, "tweet_text") %>% select(tweet_id) %>% collect() %>% sample_n(N_SAMPLE_BENCHMARK)
d_assign_benchmark


pool::dbExecute(con, "DROP TABLE IF EXISTS task_assign_benchmark")
pool::dbExecute(con,
                "
                CREATE TABLE task_assign_benchmark(
                  unique_ID TEXT NOT NULL UNIQUE
                )
                ")

pool::dbWriteTable(con, "task_assign_benchmark", d_assign_benchmark, append = TRUE)




#========================
# Task assignment
#========================

# Create table to store task assignment
pool::dbExecute(con, "DROP TABLE IF EXISTS task_assign_coder")
pool::dbExecute(con,
                "
                CREATE TABLE task_assign_coder(
                  unique_ID TEXT NOT NULL,
                  coder_id INTEGER NOT NULL,
                  assign_timestamp INTEGER,
                  condition TEXT NOT NULL
                )
                ")





#==================================
# Set up a table to store labels
#==================================

pool::dbExecute(con, "DROP TABLE tweet_label")
pool::dbExecute(con,
                "
                CREATE TABLE tweet_label(
                  unique_ID TEXT,
                  category_name TEXT,
                  label_name TEXT,
                  coder_id INTEGER,
                  label_timestamp INTEGER
                )
              ")

pool::dbExecute(con, "DROP TABLE tweet_label_unsure")
pool::dbExecute(con,
                "
                CREATE TABLE tweet_label_unsure(
                  unique_ID TEXT,
                  coder_id INTEGER,
                  label_timestamp INTEGER
                )
              ")


pool::dbExecute(con, "DROP TABLE tweet_label_missing")
pool::dbExecute(con,
                "
                CREATE TABLE tweet_label_missing(
                  unique_ID TEXT,
                  coder_id INTEGER,
                  label_timestamp INTEGER
                )
              ")
