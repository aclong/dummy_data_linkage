#this script is to automate the simulation of trnasaction data from bus timetable date

# it should load in the timetable data from the SQL db of timetables

# then it should generate versions that approximate the transaction style

# this should be done for each operator, for each route, for each day of week

###################
# Initial preparatory data
# 
# 
# R preparation input:
# Assign period of dates - 3 months in which to carry this out
# 
# Initial SQL query:
# download a list of distinct operator/route combinations
# that occur in this period
# 
# R loop preparation:
# Use the list of dates as the count/register
# For each date, for each operator/route combo, loop the following:
#
## SQL DB query:
## query the database to return all journeys from tt
##
### R loop:
### select first "unused" journey of day
### find next journey
### that is different "direction" in the tt 
### and is as close as possible 
### but does not overlap
### continue until there are no more non-overlapping opposite direction journeys that day/operator/route
### make list of "used" journey numbers
### 
###
## with data.table genreated by R Loop:
## divide stop series number by 3 to approximate the fare stage
## reverse the fare stages for each "inbound journey"
## to mimic the fare stage relationship of transaction data
## assign the data a unique "machine_ID" number
##
## SQL output data creation:
## upload a full version to SQL db
## upload a random sample of 20% of rows (to approximate transaction coverage) to another SQL db

#what time period?

#going to start with september to november 2015

start_date <- "2015-09-01"
end_date <- "2015-11-30"

#generate list
dates <- seq(as.Date(start_date), as.Date(end_date), by="days")

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

#remove table if it exists
if(dbExistsTable(con,c(dummy_data_schema, dummy_data_table))==TRUE){
  dbGetQuery(con, glue("DROP TABLE {dummy_data_schema}.{dummy_data_table};"))
  
  print("dropping old table")
}

#create new table to load results into
dbGetQuery(con, glue("CREATE TABLE {dummy_data_schema}.{dummy_data_table} ",
                     " ( ",
                     " operator_code varchar(10), ",
                     " route_no varchar(10), ",
                     " tt_direction varchar(1), ",
                     " transaction_datetime TIMESTAMP, ",
                     " fare_stage int, ",
                     " machine_id varchar(20)",
                     " ) ;"))

print("created new table")
#get all operators/routes from this period
operator_route_list <- dbGetQuery(con, glue("SELECT operator, route FROM timetables.tt_all WHERE start_date<'{end_date}' AND last_date>'{start_date}' GROUP BY operator, route ORDER BY operator, route;"))


#need unique bus numbers for each route for each day

print(glue("{Sys.time()} starting big date loop"))

#now start the big date loop
for(i in 1:length(dates)){
  
  #take the date
  this_date <- dates[i]
  
  #get the day of week
  #wday runs 1 to 7 with one being sunday so have change with lubridate week start
  this_dow <- wday(dates[i], week_start = getOption("lubridate.week.start", 1))
  
  #now a loop to cycle through the operators and routes list
  for(j in 1:nrow(operator_route_list)){
    
    #get the operator and route numbers
    this_operator <- operator_route_list$operator[j]
    
    this_route <- operator_route_list$route[j]
    
    #look at the other request
    all_journeys <- setDT(dbGetQuery(con, glue("SELECT start_date, last_date, type, TO_TIMESTAMP(CONCAT('{this_date} ',journey_scheduled), 'YYYY-MM-DD HH24:MI:SS') AS journey_scheduled, id, direction, TO_TIMESTAMP(CONCAT('{this_date} ',arrive), 'YYYY-MM-DD HH24:MI:SS') AS arrive, ROUND(n/3) AS fare_stage ",
                                               " FROM timetables.tt_all WHERE",
                                               " start_date<='{this_date}' AND last_date>='{this_date}' ",
                                               " AND SUBSTRING(dow FROM {this_dow} FOR 1)='1' ",
                                               " AND operator='{this_operator}' AND route='{this_route}';")))
    
    
    #only use latest timetable period by finding max start date
    all_journeys <- all_journeys[start_date==max(start_date),]
    
    #now only continue if all_journeys exists
    if(nrow(all_journeys)>0){
      
      #check for overlapping periods of start of end date
      start_dates <- unique(all_journeys$start_date)
      
      #loop through them
      for(period_no in 1:length(start_dates)){
        
        #get the tt_period
        tt_period <- start_dates[period_no]
        
        #subset data by period
        period_sub <- all_journeys[start_date==tt_period,,][order(journey_scheduled, id, arrive)]
        
        # get last "arrive" of every journey
        last_arrives <- period_sub[,.(last_arrive=max(arrive)), by=.(journey_scheduled, id, direction)]
        
        #this is the self joining/journey creation bit
        
        #get end of outward journeys and beginning of inward journeys 
        #and match with rolling join
        out_journey_ends <- last_arrives[direction=="O",.(id, direction, journey_scheduled, last_arrive, join_time=last_arrive),][order(join_time)]
        in_journey_starts <-  last_arrives[direction=="I",.(id, direction, journey_scheduled, last_arrive, join_time=journey_scheduled),][order(join_time)]
        out_first_join <- out_journey_ends[in_journey_starts, on='join_time', roll=Inf,  mult="first"][order(journey_scheduled)]
        
        #then rolling join inbound to next outbound journey start
        out_journey_starts <- last_arrives[direction=="O",.(id, direction, journey_scheduled, last_arrive, join_time=journey_scheduled),][,join_time_two:=journey_scheduled,][order(join_time)]
        out_first_join_end <- out_first_join[,join_time_two:=i.last_arrive,][order(join_time_two)]
        new_join <- out_first_join_end[out_journey_starts,on='join_time_two', roll=Inf, mult='first']
        
        #then get only the ids from the series
        only_ids <- na.omit(new_join[, grep("\\id\\b", names(new_join)), with = FALSE])
        
        #create blank variables for storing the output
        #used id list
        used_ids <- c()
        
        #full journeys list
        full_journeys <- list()
        
        #for loop for linking ids of sequences together
        
        ########################
        #this technique only starts with outbaound
        
        for(start_id in unique(only_ids$id)){
          
          #go through all the IDs not in the original list
          if(start_id %in% used_ids==FALSE){
            
            #get row as vector
            journey_ids <- as.matrix(only_ids[only_ids$id==start_id])[1,]
            
            #add first id to used_ids list so it doesn't get used again
            used_ids <- append(used_ids, journey_ids)
            
            #keep going when the next exists
            while(nrow(only_ids[only_ids$id==journey_ids[length(journey_ids)]])>0){
              
              new_ids <-as.matrix(only_ids[only_ids$id==journey_ids[length(journey_ids)]])[1,]
              
              #find and add next row to current journey
              journey_ids <- append(journey_ids, new_ids)
              
              #add to used ids
              used_ids <- append(used_ids, new_ids)
            }
            
            # now export the journey as a vector
            # or this is where you put in the dbQuery using the IDs to 
            # get a complete "machine journey" for the day
            #print(journey_ids)
            #print(used_ids)
            
            journey_ids <- unique(journey_ids)
            
            #myList[[length(myList)+1]] <- list(sample(1:3))
            full_journeys[[length(full_journeys)+1]] <- journey_ids
            
          }
        }
        
        #now you need to select all the journeys from the all_journeys dataset
        #assign a "machine_id" and upload to a table
        
        if(length(full_journeys)>0){
          
          machine_ids <- stri_rand_strings(length(full_journeys), 20, pattern = "[a-z0-9]")
          
          for(i in length(full_journeys)){
            
            #get machine_id
            mach_id <- machine_ids[i]
            
            #get vector of ids for this journey
            id_list <- full_journeys[[i]]
            
            #subset the all journeys dt by this vector and add machine_id column
            this_mach_journeys <- all_journeys[id %in% id_list][,.(trip_id=id, operator_code=this_operator, route_no=this_route, tt_direction=direction, transaction_datetime=arrive, fare_stage, machine_id=mach_id),]
            
            #add in bit to reverse inbound fare stage order
            #for loop running through all inbound journeys?
            inbound_ids <- unique(this_mach_journeys$trip_id[this_mach_journeys$tt_direction=="I"])
            
            #loop through the journey IDs and reverse the fare stages
            for(i in 1:length(inbound_ids)){
              rev_stages <- rev(this_mach_journeys$fare_stage[this_mach_journeys$trip_id==inbound_ids[i]])
                
              this_mach_journeys$fare_stage[this_mach_journeys$trip_id==inbound_ids[i]] <- rev_stages
            }
            
            #remove trip_id column
            this_mach_journeys <- this_mach_journeys[,trip_id:=NULL]
            
            #send to db
            dbWriteTable(con, c(dummy_data_schema, dummy_data_table), this_mach_journeys, row.names=FALSE, append=TRUE)
            
            
          }
          
          
        }
        
        
        
        
      }
      
    }
    
  }
  
}

print(glue("{Sys.time()} finished big date loop"))

############################
#Indexes to make it run faster

dbGetQuery(con, glue("CREATE INDEX ON {dummy_data_schema}.{dummy_data_table} (operator_code);"))
dbGetQuery(con, glue("CREATE INDEX ON {dummy_data_schema}.{dummy_data_table} (route_no);"))
dbGetQuery(con, glue("CREATE INDEX ON {dummy_data_schema}.{dummy_data_table} (machine_id);"))

print(glue("{Sys.time()} finished adding indexes"))

###########################
#need to create a sampled version
count_rows <- dbGetQuery(con, glue("SELECT COUNT(*) FROM {dummy_data_schema}.{dummy_data_table};"))

sample_no <- round(.2*count_rows[1,1])

#remove table if it exists
if(dbExistsTable(con,c(dummy_data_schema, glue("{dummy_data_table}_sample")))==TRUE){
  dbGetQuery(con, glue("DROP TABLE {dummy_data_schema}.{dummy_data_table}_sample;"))
  
  print("dropping old table")
}

#make new table as sample
dbGetQuery(con, glue("CREATE TABLE {dummy_data_schema}.{dummy_data_table}_sample AS ",
                     "SELECT * FROM {dummy_data_schema}.{dummy_data_table} ",
                     " ORDER BY random() ",
                     " LIMIT {sample_no};"))

print(glue("{Sys.time()} finished making sample table and starting indexes"))

#put some indexes on this one
dbGetQuery(con, glue("CREATE INDEX ON {dummy_data_schema}.{dummy_data_table}_sample (operator_code);"))
dbGetQuery(con, glue("CREATE INDEX ON {dummy_data_schema}.{dummy_data_table}_sample (route_no);"))
dbGetQuery(con, glue("CREATE INDEX ON {dummy_data_schema}.{dummy_data_table}_sample (machine_id);"))
