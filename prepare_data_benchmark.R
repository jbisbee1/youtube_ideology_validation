############################
# Load benchmark
# JN, Jen, Jim, Haohan worked on the this file together and agreed on a set of labels
# Update: 5/31
# Google sheet here: https://docs.google.com/spreadsheets/d/1fVbaH_oRR2-RZ5EFQupNToIzvseeVWU0Pb7wmRwEy18
############################


library(tidyverse)


# Load data

codebook <- readxl::read_excel("data_in/codebook/codebook.xlsx")
benchmark_raw <- readxl::read_excel("data_in/training_benchmark/benchmark.xlsx") %>%
  arrange(tweet_id)


# Relabel tweet_id: ad hoc thing
id_map <- tibble(tweet_id = unique(benchmark_raw$tweet_id), tweet_id_new = 1:50)

benchmark_raw <- benchmark_raw %>%
  inner_join(id_map) %>%
  mutate(tweet_id = tweet_id_new) %>%
  select(-tweet_id_new)


# Clean data of coded sample (separate column)

benchmark <- benchmark_raw %>%
  filter(!is.na(final)) %>%
  select(tweet_id, category_name, final) %>%
  mutate(final = str_split(final, " \\| ")) %>%
  unnest(cols = final) %>%
  rename("label_name" = "final")
  


# Check codebook - coded-sample consistency

benchmark %>% anti_join(codebook, by = c("category_name" = "category", "label_name" = "label")) %>% nrow() # OK all match



# Save finalized benchmark to SQL database

con <- pool::dbPool(RSQLite::SQLite(), dbname = "data_out/tweet_label_covid19.sqlite")

pool::dbExecute(con, "DROP TABLE tweet_label_benchmark")
pool::dbExecute(con,
                "
                CREATE TABLE tweet_label_benchmark(
                  tweet_id INTEGER,
                  category_name TEXT,
                  label_name TEXT
                )
              ")

pool::dbWriteTable(con, "tweet_label_benchmark", benchmark, append = TRUE)


#==================================
# Ad hoc: Get old benchmark's text
#==================================

con_bak = pool::dbPool(RSQLite::SQLite(), dbname = "data_out/_obselete/tweet_label_covid19_bak_20200530.sqlite")

d_benchmark_text <- tbl(con_bak, "task_assign_benchmark") %>% left_join(tbl(con_bak, "tweet_text"), by = "tweet_id") %>% 
  arrange(tweet_id) %>%
  collect() %>%
  inner_join(id_map, by = "tweet_id") %>%
  mutate(tweet_id = tweet_id_new) %>%
  select(-tweet_id_new) %>%
  arrange(tweet_id) %>%
  select(-tweet_id)


write_csv(d_benchmark_text, "data_in/training_benchmark/benchmark_text.csv")



#############################################################
# # Remove input from the testing session
# # We did a testing session to test the server's capacity. There are some random coding. Remove them.
# labels <- tbl(con, "tweet_label") %>% collect()
# labels <- labels %>%
#   mutate(coder_id = as.integer(coder_id)) %>%
#   filter(coder_id <= 4)
# 
# table(labels$coder_id)
# 
# # pool::dbExecute(con, "DROP TABLE IF EXISTS tweet_label;")
# # pool::dbExecute(con, "DROP TABLE IF EXISTS tweet_label;")
# pool::dbExecute(con,
#                 "
#                 CREATE TABLE tweet_label(
#                   tweet_id INTEGER,
#                   category_name TEXT,
#                   label_name TEXT,
#                   coder_id INTEGER,
#                   label_timestamp INTEGER
#                 )
#               ")
# 
# 
# pool::dbWriteTable(con, "tweet_label", labels, append = TRUE)
