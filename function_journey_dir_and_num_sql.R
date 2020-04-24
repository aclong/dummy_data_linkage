#write function version here
library(RPostgreSQL)
library(data.table)
library(lubridate)

#need to rewrite this one to use the SQL version. 
#Sort out when have generated a generic version from the testing file

#now make the for loop a function

#here is the initial sql command
#needs to be edited so that it groups by operator, route, machine_id and anything else?

#first get all the info from the db table on what is there
#load in the packages needed
library(data.table)
library(RPostgreSQL)
library(lubridate)
library(glue)
library(stringi)

#create db connection
#load in the env variables
db_name=Sys.getenv("DBNAME")
host_name=Sys.getenv("HOSTNAME")
user_name=Sys.getenv("USERNAME")
password_name=Sys.getenv("PASSWORDNAME")


#set up a connection
con <- dbConnect(dbDriver("PostgreSQL"), 
                 dbname = db_name, 
                 host = host_name, 
                 user = user_name, 
                 password = password_name)

#add the table name and schema name
dummy_data_schema <- "dummy_trans_tt"
dummy_data_table <- "oct_2015_dummy"

#table to create
new_table_name <- paste0(dummy_data_table, "_linked")

#get the list of cols in the table
column_names <- colnames(dbGetQuery(con, glue("SELECT * FROM {dummy_data_schema}.{dummy_data_table} LIMIT 1;")))


#strings for the window creating all grouoping and ordering
over_string_list <- c("operator_code", "route_no", "machine_id")

plain_over_strings <- paste(over_string_list, collapse=", ")

dir1_over_strings <- paste(paste0("dir1.",over_string_list), collapse=", ")

tran_string <- "transaction_datetime"

#now to create the window variables used
int_journey_window <- 20
int_stage_window <- 5


#the actual command
dbGetQuery(con, glue("CREATE TABLE {dummy_data_schema}.{new_table_name} AS ",
                                 "SELECT *, ",
                                 "CASE WHEN ",
                                 "dir1.new_direction IS NOT NULL THEN dir1.new_direction ",
                                 "WHEN dir1.new_direction IS NULL AND (dir1.fare_stage=(LAG(dir1.fare_stage, 1) OVER dir1w)) AND ((dir1.{tran_string}-(LAG(dir1.{tran_string}, 1) OVER dir1w)<INTERVAL '{int_stage_window} minutes')) THEN (LAG(dir1.new_direction, 1) OVER dir1w) ",
                                 " END AS direction ",
                                 "FROM ",
                                 "(SELECT *, ", 
                                 "CASE WHEN ( ",
                                 "((fare_stage>LAG(fare_stage, 1) OVER w) ",
                                 "AND ({tran_string}-(LAG({tran_string}, 1) OVER w)<INTERVAL '{int_journey_window} minutes') ",
                                 "AND (({tran_string}-(LAG({tran_string}, 1) OVER w)<=((LEAD({tran_string}, 1) OVER w)-{tran_string})) OR (fare_stage=LEAD(fare_stage,1) OVER w) OR ((LEAD(fare_stage,1) OVER w) IS NULL)) ) ",
                                 "OR ((fare_stage<(LEAD(fare_stage, 1) OVER w)) ",
                                 "AND ((LEAD({tran_string}, 1) OVER w)-{tran_string}<INTERVAL '{int_journey_window} minutes') ",
                                 "AND ((({tran_string}-(LAG({tran_string}, 1) OVER w)>(LEAD({tran_string}, 1) OVER w)-{tran_string})) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w))) ) ",
                                 "OR ((fare_stage<(LEAD(fare_stage, 2) OVER w)) ",
                                 "AND (fare_stage=(LEAD(fare_stage, 1) OVER w)) ",
                                 "AND ((LEAD({tran_string}, 2) OVER w)-{tran_string}<INTERVAL '{int_journey_window} minutes') ",
                                 "AND ((({tran_string}-(LAG({tran_string}, 1) OVER w))>((LEAD({tran_string}, 2) OVER w)-{tran_string})) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w))) ) ",
                                 "OR ((fare_stage<(LEAD(fare_stage, 3) OVER w)) ",
                                 "AND (fare_stage=(LEAD(fare_stage, 1) OVER w)) ",
                                 "AND (fare_stage=(LEAD(fare_stage, 2) OVER w)) ",
                                 "AND ((LEAD({tran_string}, 3) OVER w)-{tran_string}<INTERVAL '{int_journey_window} minutes') ",
                                 "AND ((({tran_string}-(LAG({tran_string}, 1) OVER w))>((LEAD({tran_string}, 3)  OVER w)-{tran_string})) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w))) ) ",
                                 ") THEN 'out' ",
                                 "WHEN (",
                                 "((fare_stage<LAG(fare_stage, 1) OVER w) AND ({tran_string}-(LAG({tran_string}, 1) OVER w)<INTERVAL '{int_journey_window} minutes') AND (({tran_string}-(LAG({tran_string}, 1) OVER w)<=((LEAD({tran_string}, 1) OVER w)-{tran_string})) OR (fare_stage=LEAD(fare_stage,1) OVER w) OR ((LEAD(fare_stage,1) OVER w) IS NULL)) ) ",
                                 "OR ((fare_stage>(LEAD(fare_stage, 1) OVER w)) AND ((LEAD({tran_string}, 1) OVER w)-{tran_string}<INTERVAL '{int_journey_window} minutes') AND ((({tran_string}-(LAG({tran_string}, 1) OVER w)>(LEAD({tran_string}, 1) OVER w)-{tran_string})) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w))) ) ",
                                 "OR ((fare_stage>(LEAD(fare_stage, 2) OVER w)) AND (fare_stage=(LEAD(fare_stage, 1) OVER w)) AND ((LEAD({tran_string}, 2) OVER w)-{tran_string}<INTERVAL '{int_journey_window} minutes') AND ((({tran_string}-(LAG({tran_string}, 1) OVER w))>((LEAD({tran_string}, 2) OVER w)-{tran_string})) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w))) ) ",
                                 "OR ((fare_stage>(LEAD(fare_stage, 3) OVER w)) AND (fare_stage=(LEAD(fare_stage, 1) OVER w)) AND (fare_stage=(LEAD(fare_stage, 2) OVER w)) AND ((LEAD({tran_string}, 3) OVER w)-{tran_string}<INTERVAL '{int_journey_window} minutes') AND ((({tran_string}-(LAG({tran_string}, 1) OVER w))>((LEAD({tran_string}, 3) OVER w)-{tran_string})) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w)) ) ) ",
                                 ") THEN 'in' ",
                                 "END AS new_direction ",
                                 "FROM {dummy_data_schema}.{dummy_data_table} ",
                                 "WINDOW w AS (PARTITION BY {plain_over_strings} ORDER BY {tran_string}) ",
                                 ") dir1 ",
                                 "WINDOW dir1w AS (PARTITION BY {dir1_over_strings} ORDER BY dir1.{tran_string});"))


assign_dir <- function(input_data_table, same_journey_mins, same_stage_mins, max_stage, min_stage){
  
  # input data table must have the columns: 
  # tran_time POSIXlt, fare_stage int
  # 
  # same_journey_mins 
  # sets how long away a transaction in the expected order must be 
  # before it is automatically excluded as being part of the same route
  # 
  # same_stage_mins 
  # sets how long away a transaction must be from the current one 
  # before it is excluded from being considered part of the same
  # 
  # min_stage and max_stage
  # these are the minimum and maximum possible fare stage for the given bus route
  # htey are used to calibrate the start and ends of journeys
  # the highest and lowest stages can be ambiguous in terms of order 
  # as they are close and appear at the beginning and end of runs 
  # so at risk of being missassigned
  #
  
  # 
  # you need to make another version of this with varying amounts
  
  # This funciont looks at transactions for a given machine on a given route, ordered by time
  # by checking the fare stage information of each transaction 
  # against the one before as well as the 3 after it, it works out the direction of travel.
  #
  # 
  
  #make sure the data table is in the right order
  input_data_table <- input_data_table[order(tran_time)]
  
  #make the journey and stage window parameters
  same_journey_window <- duration(same_journey_mins, units = "minutes")
  same_stage_window <- duration(same_stage_mins, units = "minutes")
  
  #add the new direction and journey number columns
  input_data_table$new_dir <- "blank"
  input_data_table$new_jorno <- 0
  
  #now the for loop
  for(i in 1:nrow(input_data_table)){
    print(i)
    #statement to assign direction and journey number
    if((i==1)
       && ((((input_data_table$fare_stage[i+1]-input_data_table$fare_stage[i])>0) 
            && (input_data_table$tran_time[i+1]<(input_data_table$tran_time[i]+same_journey_window)))
           | ((input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i]) 
              && ((input_data_table$fare_stage[i+2]-input_data_table$fare_stage[i])>0) 
              && (input_data_table$tran_time[i+2]<(input_data_table$tran_time[i]+same_journey_window)))
           | ((input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
              && (input_data_table$fare_stage[i+2]==input_data_table$fare_stage[i])
              && ((input_data_table$fare_stage[i+3]-input_data_table$fare_stage[i])>0) 
              && (input_data_table$tran_time[i+3]<(input_data_table$tran_time[i]+same_journey_window))))
    ){
      #checks whether any of the next three points is increasing and within journey window
      #if so assigns out
      
      direction<- "out"
    }else if((i==1)
             && ((((input_data_table$fare_stage[i+1]-input_data_table$fare_stage[i])<0) 
                  && (input_data_table$tran_time[i+1]<(input_data_table$tran_time[i]+same_journey_window)))
                 | ((input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i]) 
                    && ((input_data_table$fare_stage[i+2]-input_data_table$fare_stage[i])<0) 
                    && (input_data_table$tran_time[i+2]<(input_data_table$tran_time[i]+same_journey_window)))
                 | ((input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
                    && (input_data_table$fare_stage[i+2]==input_data_table$fare_stage[i])
                    && ((input_data_table$fare_stage[i+3]-input_data_table$fare_stage[i])<0) 
                    && (input_data_table$tran_time[i+3]<(input_data_table$tran_time[i]+same_journey_window))))){
      direction <- "in"
      
      #like above but for decreasing
      
    }else if((i==1) 
             && (input_data_table$tran_time[i+1]>(input_data_table$tran_time[i]+same_journey_window))
             && (input_data_table$tran_time[i]>(input_data_table$tran_time[i-1]+same_journey_window))){
      #checks if the transaction if too far from any other transaction to be compared
      
      direction <- "unknown"
    }else if((input_data_table$fare_stage[i]==min_stage && input_data_table$fare_stage[i-1]!=min_stage) 
             | (input_data_table$fare_stage[i]==max_stage && input_data_table$fare_stage[i-1]!=max_stage)
    ){
      #if the transaction is at the minimum or maximum farestage
      # don't know why the second part of this is in there about cheking that the previous point is not min/max stage
      direction <- "unknown"
    }else if(input_data_table$tran_time[i]==input_data_table$tran_time[i-1]){
      #sanity check for a time error in sample set 
      #(transaction time rounding led to a lower fare stage being assinged the same time as a higher one or vice versa)
      direction <- direction
    }else if(((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])<(input_data_table$tran_time[i+1]-input_data_table$tran_time[i])) | (i==nrow(input_data_table))) 
              && ((input_data_table$fare_stage[i]-input_data_table$fare_stage[i-1])>0) 
              && (input_data_table$tran_time[i]<(input_data_table$tran_time[i-1]+same_journey_window))
              && (input_data_table$fare_stage[i]>1))
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+1]-input_data_table$tran_time[i]))) 
                && (i<nrow(input_data_table))
                && ((input_data_table$fare_stage[i+1]-input_data_table$fare_stage[i])>0) 
                && (input_data_table$tran_time[i+1]<(input_data_table$tran_time[i]+same_journey_window))) 
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+2]-input_data_table$tran_time[i]))) 
                && (i<(nrow(input_data_table)-1))
                && (input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
                && ((input_data_table$fare_stage[i+2]-input_data_table$fare_stage[i])>0) 
                && (input_data_table$tran_time[i+2]<(input_data_table$tran_time[i]+same_journey_window))) 
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+3]-input_data_table$tran_time[i])))
                && (i<(nrow(input_data_table)-2))
                && (input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
                && (input_data_table$fare_stage[i+2]==input_data_table$fare_stage[i])
                && ((input_data_table$fare_stage[i+3]-input_data_table$fare_stage[i])>0) 
                && (input_data_table$tran_time[i+3]<(input_data_table$tran_time[i]+same_journey_window)))
             | ((input_data_table$fare_stage[i]!=min_stage) 
                && (input_data_table$fare_stage[i-1]==min_stage) 
                && (input_data_table$tran_time[i]<(input_data_table$tran_time[i-1]+same_journey_window)))
    ){
      #see if the direction is out
      #compares against the point before and the 3 following points 
      #for relative positions in the fare stage
      #turning this into a data.table commange would sort out the iterability of this
      direction <- "out"
    }else if(((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])<(input_data_table$tran_time[i+1]-input_data_table$tran_time[i])) | i==nrow(input_data_table)) 
              && ((input_data_table$fare_stage[i]-input_data_table$fare_stage[i-1])<0)
              && (input_data_table$tran_time[i]<(input_data_table$tran_time[i-1]+same_journey_window))) 
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+1]-input_data_table$tran_time[i]))) 
                && (i<nrow(input_data_table)) 
                && ((input_data_table$fare_stage[i+1]-input_data_table$fare_stage[i])<0) 
                && (input_data_table$tran_time[i+1]<(input_data_table$tran_time[i]+same_journey_window)))
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+2]-input_data_table$tran_time[i])))
                && (i<(nrow(input_data_table)-1))
                && (input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
                && ((input_data_table$fare_stage[i+2]-input_data_table$fare_stage[i])<0) 
                && (input_data_table$tran_time[i+2]<(input_data_table$tran_time[i]+same_journey_window)))
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+3]-input_data_table$tran_time[i]))) 
                && (i<(nrow(input_data_table)-2))
                && (input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
                && (input_data_table$fare_stage[i+2]==input_data_table$fare_stage[i])
                && ((input_data_table$fare_stage[i+3]-input_data_table$fare_stage[i])<0) 
                && (input_data_table$tran_time[i+3]<(input_data_table$tran_time[i]+same_journey_window)))
             | ((input_data_table$fare_stage[i]!=max_stage) 
                && (input_data_table$fare_stage[i-1]==max_stage) 
                && (input_data_table$tran_time[i]<(input_data_table$tran_time[i-1]+same_journey_window)))
    ){
      #like above but for fare stages going down
      
      direction <- "in"
    }else if((input_data_table$fare_stage[i]==input_data_table$fare_stage[i-1])
             && (input_data_table$tran_time[i]>(input_data_table$tran_time[i-1]+same_stage_window))
             && (input_data_table$tran_time[i+1]>(input_data_table$tran_time[i]+same_stage_window))
    ){
      #check that the transaction is within reasonable distance of the previous one
      #if not assign unkown
      
      direction <- "unknown"
    }else if(((input_data_table$fare_stage[i]==input_data_table$fare_stage[i-1]) 
              && (input_data_table$tran_time[i]<(input_data_table$tran_time[i-1]+same_stage_window)))
    ){
      #if the transaction is within the stage window of the previous one and 
      #none of the other transactions relate to it then assign same as previous
      
      direction <- direction
    }
    
    #assign direction
    input_data_table$new_dir[i] <- direction
    
    #if direction is different from previous one add 1 to journey counter
    if(i==1){
      journey_counter <- 1
    }else if(input_data_table$new_dir[i]!=input_data_table$new_dir[i-1]){
      journey_counter <- journey_counter+1
    }
    #then statement to add these to the new columns
    input_data_table$new_jorno[i] <- journey_counter
    
    
  }
  
  #return the result
  return(input_data_table)
}

