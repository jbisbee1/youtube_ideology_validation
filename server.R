library(DT)
library(openxlsx)
library(tidyverse)
library(lubridate)
library(stringr)
library(pool)

library(shinyjs)
library(V8)
library(shinycssloaders)
library(sodium) # required for password_verify


PASSWD_VIEW_BENCHMARK = "OX"
BENCHMARK_CODER_ID = 1
options(spinner.color="#0275D8", spinner.color.background="#ffffff", spinner.size=2)

# Path of database (where everything lives)
con_db <- pool::dbPool(RSQLite::SQLite(), dbname = "data_out/tweet_label.sqlite")


# # Helper function: Load Label at DB
db_load_label = function(con, unique_ID, category_name, coder_id){
  tbl(con, "tweet_label") %>%
    filter(unique_ID == !!(unique_ID) & category_name == !!(category_name) & coder_id == !!(coder_id)) %>%
    collect() %>% .$label_name
  # query = sprintf("SELECT label_name FROM tweet_label WHERE tweet_id = %s AND category_name = %s AND coder_id = %s",
  #                 tweet_id, shQuote(category_name), coder_id)
  # # print(query)
  # pool::dbGetQuery(con, query)$label_name
}

db_load_label_unsure = function(con, unique_ID, coder_id){
  tbl(con_db, "tweet_label_unsure") %>% 
    filter(unique_ID == !!(unique_ID) & coder_id == !!(coder_id)) %>%
    count() %>% collect() %>% .$n %>% as.logical()
}

db_load_label_missing = function(con, unique_ID, coder_id){
  tbl(con_db, "tweet_label_missing") %>% 
    filter(unique_ID == !!(unique_ID) & coder_id == !!(coder_id)) %>%
    count() %>% collect() %>% .$n %>% as.logical()
}



# Helper funciton: Update Label at DB
db_update_label = function(con, unique_ID, category_name, label_names, coder_id){
  if (label_names == ""){
    return()
  }
  
  if (is.na(unique_ID)){
    return()
  }
  # print("Update triggered")
  
  # Load old labels
  label_old = db_load_label(con, unique_ID, category_name, coder_id)
  
  # If old and new are both empty, quit
  if (length(label_old) == 0 & length(label_names) == 0){
    return()
  }
  
  # If old and new both have values and are the same, quit.
  if (setequal(label_old, label_names)){
    return()
  }
  
  
  # Delete old entries
  query = sprintf("DELETE FROM tweet_label WHERE unique_ID = %s AND category_name = %s AND coder_id = %s;", 
                  shQuote(unique_ID), shQuote(category_name), coder_id)
  print(query)
  pool::dbExecute(con, query)
  
  # If empty input (e.g., delete without new addition), quit
  if (is.null(label_names)) return()
  
  # Add new entry
  values = sprintf("(%s, %s, %s, %s, %s)", 
                   shQuote(unique_ID), shQuote(category_name), shQuote(label_names), coder_id, as.integer(Sys.time()))
  queries = sprintf("INSERT INTO tweet_label (unique_ID, %s, %s, %s, %s) VALUES %s;",
                    "category_name", "label_name", "coder_id", "label_timestamp",
                    paste(values, collapse = ", "))
  print(queries)
  pool::dbExecute(con, queries)
}


db_update_label_unsure = function(con, unique_ID, coder_id, label_new){
  if (is.na(unique_ID)){
    return()
  }
  label_old = db_load_label_unsure(con, unique_ID, coder_id)
  # print(sprintf("tweet_id %s old %s new %s", tweet_id, label_old, label_new))
  
  if (label_old == label_new){
    return()
  } else if (label_old == 0 & label_new == 1){
    # 0 --> 1: add
    query = sprintf("INSERT INTO tweet_label_unsure (unique_ID, coder_id, label_timestamp) VALUES (%s, %s, %s)", shQuote(unique_ID), coder_id, as.integer(Sys.time()))
    print(query)
    pool::dbExecute(con, query)
  } else if (label_old == 1 & label_new == 0){
    # 1 --> 0: delete
    query = sprintf("DELETE FROM tweet_label_unsure WHERE unique_ID == %s AND coder_id == %s", shQuote(unique_ID), coder_id)
    print(query)
    pool::dbExecute(con, query)
  }
  
}

db_update_label_missing = function(con, unique_ID, coder_id, label_new){
  if (is.na(unique_ID)){
    return()
  }
  label_old = db_load_label_missing(con, unique_ID, coder_id)
  
  if (label_old == label_new){
    return()
  } else if (label_old == 0 & label_new == 1){
    # 0 --> 1: add
    query = sprintf("INSERT INTO tweet_label_missing (unique_ID, coder_id, label_timestamp) VALUES (%s, %s, %s)", shQuote(unique_ID), coder_id, as.integer(Sys.time()))
    print(query)
    pool::dbExecute(con, query)
  } else if (label_old == 1 & label_new == 0){
    # 1 --> 0: delete
    query = sprintf("DELETE FROM tweet_label_missing WHERE unique_ID == %s AND coder_id == %s", shQuote(unique_ID), coder_id)
    print(query)
    pool::dbExecute(con, query)
  }
  
}


#========
# Login
#========

# Main login screen
loginpage <- div(id = "loginpage", style = "width: 500px; max-width: 100%; margin: 0 auto; padding: 20px;",
                 wellPanel(
                   tags$h2("LOGIN", class = "text-center", style = "padding-top: 0;color:#333; font-weight:600;"),
                   textInput("username", placeholder="Username", label = tagList(icon("user"), "Username")),
                   passwordInput("passwd", placeholder="Password", label = tagList(icon("unlock-alt"), "Password")),
                   br(),
                   div(
                     style = "text-align: center;",
                     actionButton("login", "SIGN IN", style = "color: white; background-color:#3c8dbc;
                                 padding: 10px 15px; width: 150px; cursor: pointer;
                                 font-size: 18px; font-weight: 600;"),
                     shinyjs::hidden(
                       div(id = "nomatch",
                           tags$p("Incorrect username or password.",
                                  style = "color: blue; font-weight: 600; 
                                            padding-top: 5px;font-size:16px;", 
                                  class = "text-center"))),
                     br(),
                     br()
                   ))
)



#========
# Server
#========


server = function(input, output, session){
  
  #-----------------
  # Login
  #-----------------
  
  login = FALSE
  USER <- reactiveValues(login = login)
  
  get_coder_credentials <- reactive({
    d = tbl(con_db, "coder") %>% filter(username == !!(input$username)) %>% collect()
    # print(d)
    d
  })
  
  observe({ 
    if (USER$login == FALSE) {
      if (!is.null(input$login)) {
        if (input$login > 0) {
          Username <- isolate(input$username)
          Password <- isolate(input$passwd)
          
          credentials_match <- get_coder_credentials()
          if(nrow(credentials_match)) { 
            pasmatch  <- credentials_match$passwd
            pasverify <- password_verify(pasmatch, Password)
            if(pasverify) {
              USER$login <- TRUE
            } else {
              shinyjs::toggle(id = "nomatch", anim = TRUE, time = 1, animType = "fade")
              shinyjs::delay(3000, shinyjs::toggle(id = "nomatch", anim = TRUE, time = 1, animType = "fade"))
            }
          } else {
            shinyjs::toggle(id = "nomatch", anim = TRUE, time = 1, animType = "fade")
            shinyjs::delay(3000, shinyjs::toggle(id = "nomatch", anim = TRUE, time = 1, animType = "fade"))
          }
        } 
      }
    }    
  })
  
  output$logoutbtn <- renderUI({
    req(USER$login)
    # # I wanted a button. But it does not display. May improve later.
    # tags$li(a(icon("fa fa-sign-out"), "Click here to logout",
    #           href="javascript:window.location.reload(true)"),
    #         class = "dropdown",
    #         style = "background-color: #eee !important; border: 0;
    #                 font-weight: bold; margin:5px; padding: 10px;")
    tags$a(icon("fa fa-sign-out"), "Click here to logout",
           href="javascript:window.location.reload(true)")
    
  })
  
  # Add downloadbutton for instruciton
  output$download_instruction <- downloadHandler(
    filename = "smapp_instruction_general.pdf",
    content = function(file){
      file.copy("data_in/codebook/Tweet_Labelling_Instructions.pdf", file)
    }
  )
  
  # output$download_faq <- downloadHandler(
  #   filename = "smapp_faq.pdf",
  #   content = function(file){
  #     file.copy("data_in/codebook/COVID_coding_FAQ.pdf", file)
  #   }
  # )
  # 
  # output$download_instruction_experiment <- downloadHandler(
  #   filename = "smapp_instruction_experiment.pdf",
  #   content = function(file){
  #     file.copy("data_in/codebook/COVID_Coding_Supplement.pdf", file)
  #   }
  # )
  
  
  
  #-------
  # * UI
  #-------
  
  output$tab_coder_summary <- renderUI({
    fluidPage(
      fluidRow(
        column(12, align = "center", h2("Hi", tags$strong(get_coder_credentials()$screename), "! You have logged in.")),
        column(12, align = "center", h3(uiOutput("logoutbtn")))
      ),
      tags$br(),
      fluidRow(column(12, align = "center",
                      downloadButton("download_instruction", "Download General Instruction")
                      #downloadButton("download_faq", "Download FAQ"),
                      #downloadButton("download_instruction_experiment", "Download Experiment Instruction")
                      ), 
      ),
      
      tags$br(),
      column(12, align = "center", h3("Summary of Your Coding Activities")),
      tags$p(),
      column(12, align = "center", actionButton("update_summary_coder", "Update Report")),
      tags$p(),
      tags$p(),
      fluidRow(column(12, align = "center", tableOutput("completion_rate_individual") %>% withSpinner())),
      tags$br(),
      column(12, align = "center", plotOutput("label_timestamp_individual", width = "600px", height = "300px") %>% withSpinner()),
      conditionalPanel(
        # condition = sprintf("input.passwd_check_answer != '%s' && '%s' != 'coder'", PASSWD_VIEW_BENCHMARK, get_coder_credentials()$permission), 
        condition = sprintf("input.passwd_check_answer != '%s'", PASSWD_VIEW_BENCHMARK), 
        tags$br(),
        tags$br(),
        tags$br(),
        column(12, align = "center", h4("[For Training Session]")),
        column(12, align = "center", textInput("passwd_check_answer", "Enter Password to Check Your Answers", ""))
      )
    )
    
  })
  
  output$tab_codebook <- renderUI({
    fluidPage(
      h3("Codebook"),
      DT::dataTableOutput("label_table") %>% withSpinner()
    )
  })
  
  
  output$tab_coder_progress <- renderUI({
    fluidPage(
      h2(paste0("Summary Statistics of Coders' Activities")),
      tags$br(),
      actionButton("update_summary_admin", "Update Report"),
      h3("Coders' Activities"),
      column(12, align = "center", tableOutput("completion_rate") %>% withSpinner()),
      h3("Distribution of Label Assignment by Coder"),
      column(12, align = "center", plotOutput("label_freq_bar", width = "600px", height = "1400px")  %>% withSpinner()),
      h3("When coders work"),
      column(12, align = "center", plotOutput("label_timestamp", width = "600px", height = "300px") %>% withSpinner())
    )    
  })
  
  output$tab_view_benchmark_admin <- renderUI({
    fluidPage(
      tags$br(),
      actionButton("update_summary_admin_table", "Update Table"),
      tags$p(),
      DT::dataTableOutput("table_tweets") %>% withSpinner()
      
    )
  })
  
  output$tab_tweet_coding <- renderUI({
    if (USER$login == TRUE){
      # permission = get_coder_credentials()$permission
      # print(permission)
      
      fluidPage(
        tags$head(
          tags$script(async = NA, src = "https://platform.twitter.com/widgets.js")
        ),
        useShinyjs(), 
        extendShinyjs(text = "shinyjs.ButtonToTop = function() {window.scrollTo(0, 0);}", functions = c("ButtonToTop")), # Scroll to top function
        tags$br(),
        fluidRow(
          column(4, align = "center", uiOutput("num_of_tweet_page")),
          column(4, align = "center", selectInput("coding_filter_tweets", label = "Filter", choices = c("All", "Unsure", "Not Labelled"), selected = "Not Labelled")),
          column(4, align = "center", uiOutput("page_nav"))
        ),
        # tags$br(),
        tags$br(),
        column(12, align = "center",
               actionButton("tweet_prev_page", "PREV PAGE"),
               actionButton("tweet_next_page", "NEXT PAGE")),
        tags$br(),
        tags$br(),
        column(12, align = "center", uiOutput("update_note")),
        tags$br(),
        conditionalPanel(
          tags$br(),
          condition = sprintf("input.passwd_check_answer == '%s'", PASSWD_VIEW_BENCHMARK),
          column(12, align = "center", actionButton("update_check_answer", "Refresh Tables")),
          tags$br()
        ),
        tags$hr(),
        tags$br(),
        uiOutput("coding_panel") %>% withSpinner(),
        tags$br(),
        column(12, align = "center",
               actionButton("tweet_prev_page2", "PREV PAGE"),
               actionButton("tweet_next_page2", "NEXT PAGE")),
        tags$br(),
        tags$br(),
        tags$br()
      )
      
    }
  })
  
  
  # output$tab_training <- renderUI({
  #   tabsetPanel(
  #     tabPanel("Attempt Benchmark", uiOutput("tab_tweet_coding")),
  #     tabPanel("Check Answers", uiOutput("tab_benchmark_check_answer"))
  #   )
  #   
  # })
  
  output$main_page <- renderUI({
    TITLE = "Video Labeler"
    if (USER$login == TRUE){
      
      coder_credentials <- get_coder_credentials()
      
      if (coder_credentials$permission == "admin"){
        navbarPage(
          TITLE,
          id = "tabs",
          tabPanel("Login", uiOutput("tab_coder_summary")),
          tabPanel("Codebook", uiOutput("tab_codebook")),
          tabPanel("Coding Panel", uiOutput("tab_tweet_coding")),
          tabPanel(tags$b(tags$span("Track Progress", style = "color:blue")), uiOutput("tab_coder_progress"))
        )
      } else if (coder_credentials$permission == "coder-training") {
        navbarPage(
          TITLE,
          id = "tabs",
          tabPanel("Login", uiOutput("tab_coder_summary")),
          tabPanel("Codebook", uiOutput("tab_codebook")),
          tabPanel("Coding Panel", uiOutput("tab_tweet_coding"))
        )
      } # else if (coder_credentials$permission == "coder"){
      #   navbarPage(
      #     TITLE,
      #     id = "tabs",
      #     tabPanel("Login", uiOutput("tab_coder_summary")),
      #     tabPanel("Codebook", uiOutput("tab_codebook")),
      #     # tabPanel("View Training Results", uiOutput("tab_benchmark_check_answer")),
      #     tabPanel("Coding Panel", uiOutput("tab_tweet_coding"))
      #   )
      # }
    } else{
      navbarPage(TITLE, tabPanel("Login", useShinyjs(), loginpage))
    }
    
  })
  
  
  
  
  #-----------------
  # Labels management
  #-----------------
  
  # EXPERIMENT: This is the codebook
  read_label = reactive({
    tbl(con_db, "codebook") %>% select(-codebook_id) %>% collect()
  })
  
  ## read_label_left = reactive
  ## read_label_right = 
  
  output$label_table = DT::renderDataTable(
    datatable(read_label(),
              class = 'stripe hoover',
              escape = FALSE, editable = FALSE, rownames = FALSE,
              extensions = c('Buttons', 'FixedHeader', 'ColReorder'),
              filter = 'top',
              options = list(dom = 'Bfrtip',
                             searchHighlight = TRUE,
                             buttons = c('colvis', 'pageLength', 'copy', 'csv', 'excel', 'pdf', 'print'),
                             pageLength = 100, fixedHeader = FALSE,
                             colReorder = TRUE)
    )
  )
  
  
  #-------------------
  # Get tweets
  #-------------------
  
  get_tweets = eventReactive(input$username, {
    coder_id = get_coder_credentials()$coder_id
    
    d_benchmark = tbl(con_db, "task_assign_benchmark")
    
    print("Load assignment")
    d_assign = tbl(con_db, "task_assign_coder") %>%
      filter(coder_id == !!(coder_id))
    d_assign = d_benchmark %>% union_all(d_assign)      
    
    d_out <- d_assign %>% arrange(assign_timestamp, unique_ID) %>% collect()
    
    d_out    
    
    # EXPERIMENT: add a column "condition
  })
  
  
  filter_tweets = eventReactive(input$coding_filter_tweets, {
    req(get_tweets())
    
    if (input$coding_filter_tweets == "All"){
      get_tweets()
    } else if (input$coding_filter_tweets == "Unsure"){
      id_unsure = tbl(con_db, "tweet_label_unsure") %>% 
        filter(coder_id == !!(get_coder_credentials()$coder_id)) %>%
        select(unique_ID) %>%
        collect()
      
      get_tweets() %>% semi_join(id_unsure, by = "unique_ID")
      
    } else if (input$coding_filter_tweets == "Not Labelled"){
      id_done = tbl(con_db, "tweet_label") %>%
        filter(coder_id == !!(get_coder_credentials()$coder_id)) %>%
        select(unique_ID) %>%
        distinct() %>%
        collect()
      
      print(id_done)
      
      print(get_tweets() %>% anti_join(id_done, by = "unique_ID"))
      
      get_tweets() %>% anti_join(id_done, by = "unique_ID")
      
    }
  })
  
  
  #-------------------
  # Benchmark
  #-------------------
  
  f <- function(x){
    xs <- strsplit(as.character(x), "")[[1]]
    paste0(sprintf("&#%d;", sapply(xs, utf8ToInt)), collapse="")
  }
  
  
  observeEvent(page_change(), {
    
    observeEvent(req(input$passwd_check_answer == PASSWD_VIEW_BENCHMARK), { # Note this req() !!
      observeEvent(input$update_check_answer, {
        
        tweet_id_current_page = filter_tweets()$unique_ID[tweet_row_start():tweet_row_end()]
        
        # Experiment idea 2: Load different labels
        d_label_all = tbl(con_db, "tweet_label") %>%
          filter(unique_ID %in% !!(tweet_id_current_page)) %>%
          select(unique_ID, category_name, label_name, coder_id)
        
        d_label_self = d_label_all %>%
          filter(coder_id == !!(get_coder_credentials()$coder_id)) %>%
          select(-coder_id) %>%
          collect() %>%
          mutate(type = "Your Label")
        
        if (get_coder_credentials()$permission == "admin"){
          # Load unsure data
          d_unsure = tbl(con_db, 'tweet_label_unsure') %>%
            left_join(tbl(con_db, "coder") %>% select(coder_id, username), by = "coder_id") %>%
            rename('type'= 'username') %>%
            mutate(type = ifelse(coder_id == !!(get_coder_credentials()$coder_id), "Your Label", type)) %>%
            mutate(
              category_name = '_Unsure_', label_name = '1'
            ) %>%
            select(unique_ID, category_name, label_name, type)
          print(d_unsure)
          
          d_label_other = d_label_all %>%
            filter(coder_id != !!(get_coder_credentials()$coder_id)) %>%
            left_join(tbl(con_db, "coder") %>% select(coder_id, username), by = "coder_id") %>% # NEW
            select(-coder_id) %>% # NEW
            rename("type" = "username") %>%
            mutate(type = as.character(type)) %>%
            union_all(d_unsure) %>%
            collect()
          print(d_label_other)
          
        } else if (get_coder_credentials()$permission != "admin"){
          d_label_other = d_label_all %>%
            filter(coder_id == !!(BENCHMARK_CODER_ID)) %>%
            select(-coder_id) %>%
            collect() %>%
            mutate(type = "Benchmark")
        }
        
        # if (input$coding_filter_tweets == "RA unsure"){
        #   d_unsure = tbl(con_db, 'tweet_label_unsure') %>%
        #     left_join(tbl(con_db, "coder") %>% select(coder_id, username), by = "coder_id") %>%
        # }
        
        if (nrow(d_label_self) > 0){
          d = d_label_self %>%
            bind_rows(d_label_other) %>%
            group_by(unique_ID, category_name, type) %>%
            arrange(label_name) %>%
            summarise(label_name = paste(label_name, collapse = " | ")) %>%
            ungroup() %>%
            arrange(type) %>%
            pivot_wider(names_from = type, values_from = label_name) %>%
            mutate_all(~ifelse(is.na(.), "", .)) %>%
            rename("category" = "category_name")
          
          if (has_name(d, "Benchmark") & has_name(d, "Your Label")){
            d = d %>%
              mutate(Agree = ifelse(`Benchmark` == `Your Label`, "✔️", "❌"))
          }
        } else{
          d = d_label_self
        }
        
        # Render tables checking answer
        lapply(1:length(tweet_id_current_page), function(i) {
          row_id = tweet_row_start() - 1 + i
          out <- d %>% filter(unique_ID == !!(tweet_id_current_page[i])) %>% select(-unique_ID)
          # Transpose if admin (too long)
          if (nrow(out) > 0 & get_coder_credentials()$permission == "admin"){
            out = out %>%
              gather(username, value, -category) %>%
              spread(category, value)
          }
          
          output[[paste0("benchmark_check_", row_id)]] = renderTable({
            if (nrow(out) > 0){
              out
            } else{
              NULL
            }
            # d %>% filter(tweet_id == !!(tweet_id_current_page[i])) %>% select(-tweet_id)
          }, width = "100%", striped = TRUE, sanitize.text.function = function(x) sapply(x, f))
        })
      }, ignoreNULL = FALSE)
    })
  })
  
  
  
  #--------------------------
  # Progress Tracking
  #--------------------------
  
  # For admins
  observeEvent(input$update_summary_admin, {
    sum_unsure = tbl(con_db, "tweet_label_unsure") %>% group_by(coder_id) %>% 
      summarise(`Unsure` = n()) %>% collect()
    
    sum_assign = tbl(con_db, "task_assign_coder") %>% group_by(coder_id) %>%
      summarise(`Assigned` = n() + 200L) %>% collect()
    
    sum_stats = tbl(con_db, "tweet_label") %>%
      group_by(coder_id, unique_ID) %>%
      summarise(`First Activity` = min(label_timestamp, na.rm = F), 
                `Latest Activity` = max(label_timestamp, na.rm = F)) %>%
      group_by(coder_id) %>%
      summarise(`First Activity` = min(`First Activity`, na.rm = F), 
                `Latest Activity` = max(`Latest Activity`, na.rm = F), 
                `N Tweets Done` = n()) %>%
      collect() %>%
      mutate_at(vars(`First Activity`, `Latest Activity`), 
                ~as.character(as_datetime(., tz = "America/New_York"))) %>%
      full_join(sum_assign, by = "coder_id") %>%
      full_join(sum_unsure, by = "coder_id")
    
    sum_stats <- tbl(con_db, "coder") %>% select(coder_id, screename) %>% collect() %>%
      left_join(sum_stats, by = "coder_id") %>% mutate_at(vars(`N Tweets Done`, `Assigned`, `Unsure`), ~ifelse(is.na(.), 0L, .))
    
    output$completion_rate = renderTable({
      sum_stats
    }, caption = paste(now("America/New_York"), "EDT", collapse = " "))
    
    output$label_freq_bar = renderPlot({
      tbl(con_db, "tweet_label") %>%
        group_by(coder_id, label_name, category_name) %>%
        count() %>%
        ungroup() %>%
        collect() %>%
        mutate(coder_id = factor(coder_id)) %>%
        ggplot(aes(x = n, y = label_name)) +
        geom_bar(aes(fill = coder_id), position = "stack", stat = "identity") +
        facet_grid("category_name", scales = "free", space = "free") +
        theme_bw() +
        theme(strip.text.y.right = element_text(angle = 0), 
              legend.position = "top",
              text = element_text(size=15)) +
        ggtitle(paste(now("America/New_York"), "EDT", collapse = " "))
      
    })
    
    output$label_timestamp = renderPlot({
      tbl(con_db, "tweet_label") %>%
        collect() %>%
        mutate(label_timestamp = as_datetime(label_timestamp, tz = "America/New_York"),
               coder_id = factor(coder_id)) %>%
        ggplot(aes(x = label_timestamp)) +
        geom_histogram(aes(fill = coder_id), position = "stack", bins = 40) +
        theme_bw() +
        theme(legend.position = "top", text = element_text(size=15)) +
        ggtitle(paste(now("America/New_York"), "EDT", collapse = " "))
    })
  }, ignoreNULL = FALSE)
  
  
  # For individual coders
  observeEvent(input$update_summary_coder, {
    
    sum_unsure = tbl(con_db, "tweet_label_unsure") %>% group_by(coder_id) %>% 
      summarise(`Unsure` = n()) %>% collect()
    
    sum_assign = tbl(con_db, "task_assign_coder") %>% group_by(coder_id) %>%
      summarise(`Assigned` = n()) %>% collect()
    
    sum_stats = tbl(con_db, "tweet_label") %>%
      group_by(coder_id, unique_ID) %>%
      summarise(`First Activity` = min(label_timestamp, na.rm = F), 
                `Latest Activity` = max(label_timestamp, na.rm = F)) %>%
      group_by(coder_id) %>%
      summarise(`First Activity` = min(`First Activity`, na.rm = F), 
                `Latest Activity` = max(`Latest Activity`, na.rm = F), 
                `N Tweets Done` = n()) %>%
      collect() %>%
      mutate_at(vars(`First Activity`, `Latest Activity`), 
                ~as.character(as_datetime(., tz = "America/New_York"))) %>%
      full_join(sum_assign, by = "coder_id") %>%
      full_join(sum_unsure, by = "coder_id")
    
    sum_stats <- tbl(con_db, "coder") %>% select(coder_id, screename) %>% collect() %>%
      left_join(sum_stats, by = "coder_id") %>% 
      mutate_at(vars(`N Tweets Done`, `Assigned`, `Unsure`), ~ifelse(is.na(.), 0L, .)) %>%
      mutate(`Assigned` = `Assigned` + 200L)
    
    output$completion_rate_individual = renderTable({
      coder_id = get_coder_credentials()$coder_id
      # print(coder_id)
      
      sum_stats %>% filter(coder_id == !!(coder_id)) %>% select(-coder_id, -screename)
      # 
      # tbl(con_db, "tweet_label") %>%
      #   filter(coder_id == !!(coder_id)) %>%
      #   group_by(tweet_id) %>%
      #   summarise(`First Activity` = min(label_timestamp, na.rm = F), 
      #             `Latest Activity` = max(label_timestamp, na.rm = F)) %>%
      #   ungroup() %>%
      #   summarise(`First Activity` = min(`First Activity`, na.rm = F), 
      #             `Latest Activity` = max(`Latest Activity`, na.rm = F), 
      #             `Num Tweets Coded` = n()) %>%
      #   collect() %>%
      #   mutate_at(vars(`First Activity`, `Latest Activity`), 
      #             ~as.character(as_datetime(., tz = "America/New_York")))
    })
    
    output$label_timestamp_individual = renderPlot({
      tbl(con_db, "tweet_label") %>%
        collect() %>%
        filter(coder_id == !!(get_coder_credentials()$coder_id)) %>%
        mutate(timestamp = as_datetime(label_timestamp, tz = "America/New_York")) %>%
        ggplot(aes(x = timestamp)) +
        geom_histogram(position = "stack", bins = 40) +
        theme_bw() +
        theme(legend.position = "top", text = element_text(size=15))
    })
  }, ignoreNULL = FALSE)
  
  
  
  #-----------------
  # Label tweets
  #-----------------
  
  observeEvent(input$username, hideTab(inputId = "tabs", target = "Coding Videos"))
  observeEvent(input$username, hideTab(inputId = "tabs", target = "Codebook"))
  
  observeEvent(input$submit_coder_id, showTab(inputId = "tabs", target = "Codebook"))
  observeEvent(input$submit_coder_id, showTab(inputId = "tabs", target = "Coding Videos"))
  
  
  # Specify number of tweets per page
  output$num_of_tweet_page = renderUI({
    sliderInput("n_tweet_page", "Number of videos per Page", min = 10, max = 30, step = 5, value = 10)
  })
  
  # Locate page number
  max_page = reactive(ceiling(nrow(filter_tweets()) / input$n_tweet_page))
  
  output$page_nav = renderUI({
    sliderInput("page_num", "Page Number", min = 1, max = max_page(), value = 1)
  })
  
  observeEvent(input$tweet_prev_page,{
    req(input$page_num)
    js$ButtonToTop()
    current = input$page_num
    print(paste("CURRENT PAGE:", current))
    updateSliderInput(session, "page_num", value = max(current-1, 1))
  })
  
  observeEvent(input$tweet_next_page,{
    req(input$page_num)
    js$ButtonToTop()
    current = input$page_num
    print(paste("CURRENT PAGE:", current))
    updateSliderInput(session, "page_num", value = min(current+1, max_page()))
  })
  
  observeEvent(input$tweet_prev_page2,{
    req(input$page_num)
    js$ButtonToTop()
    current = input$page_num
    print(paste("CURRENT PAGE:", current))
    updateSliderInput(session, "page_num", value = max(current-1, 1))
  })
  
  observeEvent(input$tweet_next_page2,{
    req(input$page_num)
    js$ButtonToTop()
    current = input$page_num
    print(paste("CURRENT PAGE:", current))
    updateSliderInput(session, "page_num", value = min(current+1, max_page()))
  })
  
  # Coding panels for tweets
  label_list = reactive({
    categories = unique(read_label()$category)
    labels = lapply(categories, function(x){
      read_label() %>% filter(category == x) %>% .$label
    })
    names(labels) = categories
    labels
  })
  
  tweet_row_start = reactive((input$page_num - 1) * input$n_tweet_page + 1)
  tweet_row_end = reactive(min(input$page_num * input$n_tweet_page, nrow(filter_tweets())))
  
  
  page_change = reactive({
    req(input$page_num) # Wait for its loading
    req(input$n_tweet_page) # Wait for its loading
    req(input$coding_filter_tweets)
    c(input$page_num, input$n_tweet_page, input$coding_filter_tweets)
    # print("PAGE CHANGE or CHANGE FILTER")
  })
  
  # Define style of the coding panel
  style_menu = reactive({
    req(label_list())
    MENU_PER_ROW = 2
    width_per_menu = 12 / MENU_PER_ROW
    menu_nrow = ceiling(length(label_list()) / MENU_PER_ROW)
    c(MENU_PER_ROW, width_per_menu, menu_nrow)
  })
  
  output$coding_panel = renderUI({
    req(page_change())
    
    # docs = filter_tweets()
    coder_id = get_coder_credentials()$coder_id
    
    tweet_id_this_page = filter_tweets() %>% slice(tweet_row_start():tweet_row_end()) %>% .$unique_ID
    
    docs = tbl(con_db, "tweet_text") %>% filter(unique_ID %in% tweet_id_this_page) %>% collect()
    docs <- tibble(unique_ID = tweet_id_this_page) %>% inner_join(docs, by = "unique_ID")
    
    docs <- docs %>% left_join(get_tweets())
    
    # # Merge URLs into a list column
    # docs$url = docs %>% select(tweet_id, url_0:url_6) %>%
    #   gather(key, value, -tweet_id) %>%
    #   group_by(tweet_id) %>%
    #   summarise(url = list(value[!is.na(value)])) %>%
    #   .$url
    # 
    if (nrow(docs) == 0){
      fluidRow(column(12, align = "center", h3("No video in this group.")))
    } else {
      lapply(1:nrow(docs), function(x) {
        row_id = tweet_row_start() - 1 + x
        
        fluidRow(
          fluidRow(column(12, strong(docs$unique_ID[x]))#,
                   #conditionalPanel(condition = sprintf("%s <= 50", docs$tweet_id[x]), # Ad hoc. first 50 tweets
                   #                  column(12, "[training]"))
          ),
          tags$br(),
          # fluidRow(column(12, docs$timestamp[x])),
          # tags$br(),
          fluidRow(
            column(6, 
                   # fluidRow(
                   #   tags$br(),
                   #   column(12, docs$tweet_text[x]),
                   #   tags$br(),
                   #   tags$br(),
                   # ),
                   # tags$br(),
                   # conditionalPanel(
                   #   condition = sprintf("%s == 1", ifelse(!is.na(docs$quoted__tweet_id[x]), 1, 0)),
                   #   fluidRow(
                   #     column(12, strong(sprintf("Quoted text %s", docs$quoted__tweet_id[x]))),
                   #     tags$br(),
                   #     column(12, docs$quoted__text[x])
                   #   )
                   # ),
                   # tags$br(),
                   fluidRow(
                     # Embedded tweet!!! Change video id here --
                     column(12, align = "center", 
                            # tagList(
                            #   tags$blockquote(class = "twitter-tweet",
                            #                   tags$a(href = sprintf("https://twitter.com/web/status/%s", docs$tweet_id[x]))),
                            #   # tags$a(href = sprintf("https://twitter.com/i/status/%s", docs$id_str[x]))),
                            #   tags$script('twttr.widgets.load(document.getElementById("tweet"));')
                            # )
                      # Embed Youtube video
                      # HTML('<iframe width="560" height="315" src="https://www.youtube.com/embed/T1-k7VYwsHg" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>')
                      HTML(sprintf('<iframe width="560" height="315" src="https://www.youtube.com/embed/%s" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>',docs$tweet_id_T[x])),
                      HTML(sprintf('<iframe width="560" height="315" src="https://www.youtube.com/embed/%s" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>',docs$tweet_id_B[x]))
                     ),
                     column(12,
                            checkboxInput(paste0("tweet_label_missing_", row_id), "Missing or unaccessible video",
                                          value = db_load_label_missing(con_db, unique_ID = docs$unique_ID[x],
                                                                        coder_id = get_coder_credentials()$coder_id))
                     )
                   ),
            ),
            column(6,
                   tableOutput(paste0("benchmark_check_", row_id)),     
                   lapply(1:style_menu()[3], function(y){
                     fluidRow(
                       lapply(((y - 1) * style_menu()[1] + 1):min((y - 1) * style_menu()[1] + style_menu()[1], length(label_list())), function(z){
                         
                         label_selected <- db_load_label(con_db,
                                                         unique_ID = docs$unique_ID[x],
                                                         category_name = names(label_list())[z],
                                                         coder_id = coder_id)
                         
                         # Updated 6/18
                         label_all <- label_list()[z][[1]]
                         label_all <- unique(c(label_all, label_selected)) # To revisit later. 
                         if(docs$condition[z] == "L") {
                           label_all <- unique(label_all[which(grepl(' left leaning|not ideological',label_all))])
                         } else {
                           label_all <- unique(label_all[which(grepl(' right leaning|not ideological',label_all))])
                         }
                         print(label_selected)
                         # EXPERIMENT: Add label_right, lebel_left
                         # print(docs)
                         column(style_menu()[2],
                                # EXPERIMENT: add condiional panel here --- (wrap the selectizeinput with conditional panel)
                                selectizeInput(paste0("tweet_label_", z, "_", row_id),
                                               names(label_list())[z],
                                               choices = label_all,
                                               size = '100%',
                                               multiple = FALSE,
                                               options = list(placeholder = '', create = T),
                                               selected = label_selected
                                )
                         )
                         
                       })
                     )
                   })#,
                   # fluidRow(column(12, 
                   #                 checkboxInput(paste0("tweet_label_unsure_", row_id), "I'm not sure.",
                   #                               value = db_load_label_unsure(con_db, tweet_id = docs$tweet_id[x], 
                   #                                                            coder_id = get_coder_credentials()$coder_id))
                   # )
                   # )
            )
          ),
          tags$hr()
        )
      })
    }
  })
  
  
  
  observeEvent(page_change(), {
    i_first = tweet_row_start()
    i_last = tweet_row_end()
    
    tab_ij = expand.grid(1:length(label_list()), i_first:i_last)
    # print(tab_ij)
    
    apply(tab_ij, 1, function(x){
      observeEvent(input[[paste0("tweet_label_", x[1], "_", x[2])]],{
        unique_ID = filter_tweets()$unique_ID[x[2]]
        category = names(label_list())[x[1]]
        labels = input[[paste0("tweet_label_", x[1], "_", x[2])]]
        db_update_label(con = con_db, unique_ID, category, labels, get_coder_credentials()$coder_id)
        
        output$update_note = renderUI({
          sprintf("Updated video ID %s: labels %s", unique_ID,
                  paste(labels, collapse = ", "))
        })
      }, ignoreNULL = FALSE, ignoreInit = TRUE)      
    })
    
  })
  
  observeEvent(page_change(), {
    i_first = tweet_row_start()
    i_last = tweet_row_end()
    
    lapply(tweet_row_start():tweet_row_end(), function(x){
      observeEvent(input[[paste0("tweet_label_unsure_", x)]], {
        unique_ID = filter_tweets()$unique_ID[x]
        label_unsure = input[[paste0("tweet_label_unsure_", x)]]
        
        db_update_label_unsure(con = con_db, unique_ID, get_coder_credentials()$coder_id, label_unsure)
      }, ignoreInit = TRUE)
    })
    
    lapply(tweet_row_start():tweet_row_end(), function(x){
      observeEvent(input[[paste0("tweet_label_missing_", x)]], {
        unique_ID = filter_tweets()$unique_ID[x]
        label_missing = input[[paste0("tweet_label_missing_", x)]]
        
        db_update_label_missing(con = con_db, unique_ID, get_coder_credentials()$coder_id, label_missing)
      }, ignoreInit = TRUE)
    })
  })
  
}