library(tidyverse)
library(RSQLite)
library(pool)


con <- pool::dbPool(RSQLite::SQLite(), dbname = "data_out/tweet_label_covid19.sqlite")

ls_tables <- db_list_tables(con)[c(1, 2, 6, 8, 9)]

path <- "../../data-analysis/"

list.files(path)

for (table_name in ls_tables){
  tbl(con, table_name) %>%
    collect() %>%
    write_csv(., paste0(path, table_name, ".csv"))
}


experiment_tweets <- tbl(con, "tweet_label") %>% select(tweet_id) %>% distinct() %>%
  filter(tweet_id > 50) %>%
  left_join(tbl(con, "tweet_text")) %>%
  collect() %>%
  write_csv(paste0(path, "_out_tweet_text_in_experiment.csv"), na = "")

