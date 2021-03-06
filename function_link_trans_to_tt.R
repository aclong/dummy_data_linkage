#attach tt_id to transaction data

#packages
library(RPostgreSQL)
library(data.table)
library(glue)
library(lubridate)

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
dummy_data_table <- "oct_2015_dummy_sample"

#linked data table name
new_table_name <- paste0(dummy_data_table, "_linked")

#possible stop table name
record_vs_codes <- "record_vs_codes"

#transaction var name
tran_string <- "transaction_datetime"

#see if table exists and if yes delete and recreate
#check if it exists and if so delete
#remove table if it exists
if(dbExistsTable(con,c(dummy_data_schema, record_vs_codes))==TRUE){
  dbGetQuery(con, glue("DROP TABLE {dummy_data_schema}.{record_vs_codes};"))
  
  print("dropping old table")
}

#make the table
dbGetQuery(con, glue("CREATE TABLE {dummy_data_schema}.{record_vs_codes} (record_id int, time_join_stops TEXT [], prop_join_stops TEXT [], tt_id varchar);"))




#start a for loop to go through all the machines

#the data is all between september and november 2015 so no need to sample

#stipulate only the varaiables you need
trans_vars_string <- "operator_code, route_no, transaction_datetime, direction, journey_number, record_id, journey_proportion"

tt_vars_string <- " start_date, journey_scheduled, id, direction, naptan_code, n, arrive, dow"



#get all the machine IDs
all_mach_ids <- dbGetQuery(con, glue("SELECT DISTINCT machine_id FROM {dummy_data_schema}.{new_table_name}_journey_no;"))

#length(all_mach_ids$machine_id)

###########################
#new version

system.time(
for(id_num in 1:length(all_mach_ids$machine_id)){

  #takes a machine id
  machine_id <- all_mach_ids$machine_id[id_num]

  #gets all transactions for that data
  one_machine_transaction_data <- setDT(dbGetQuery(con, glue("SELECT {trans_vars_string} FROM {dummy_data_schema}.{new_table_name}_journey_no ",
                                                             " WHERE machine_id='{machine_id}' ",
                                                             "AND direction!='NA' ",
                                                             " ORDER BY {tran_string};")))
  
  #check that this returned a result and only continue if so
  if(nrow(one_machine_transaction_data)>0){
  
    #breaks down the data on that machine by journey number
    
    #actually break down journeys by operator code and transaction as well
    journey_nums <- unique(one_machine_transaction_data[, .(operator_code, route_no, journey_number), nomatch=0 ])
    
    if(id_num%%1000==0){
      print(glue("Working on machine number {id_num} at {Sys.time()} "))
      print(glue("It has {length(journey_nums)} journeys"))
    }
    
    #then go through all the journeys
    for(i in 1:nrow(journey_nums)){
      
      #route and operator info
      this_operator_code <- journey_nums$operator_code[i]
      this_route_no <- journey_nums$route_no[i]
      
      #get only one journey
      one_journey <- one_machine_transaction_data[journey_number==journey_nums$journey_number[i] & route_no==this_route_no & operator_code==this_operator_code,]
      
      #check that there is only one journey in this section
      #might not need this bit if I sort out the journey number assigning cycle
      
      #get min and max times for the journey starting
      min_datetime <- min(one_journey$transaction_datetime)-hours(1)
      
      max_datetime <- max(one_journey$transaction_datetime)+hours(1)
      
      #in case the journey breaks over differen weekdays
      #get dow info
      #only need the first one
      min_dow <- wday(min_datetime, week_start = getOption("lubridate.week.start", 1))
      
      
      #direction info
      if(one_journey$direction[1]=="in"){
        tt_direction <- "I"
      }else if(one_journey$direction[1]=="out"){
        tt_direction <- "O"
      }
      
      #get all possible journeys from tt
      #start using only min_dow as the times from that date should roll over in the tt
      #FIND OUT ABOUT THIS
      
      tt_testers <- setDT(dbGetQuery(con, glue("SELECT {tt_vars_string} FROM timetables.tt_all ",
                                               "WHERE start_date<='{format(max_datetime, '%Y-%m-%d')}' ",
                                               "AND operator='{this_operator_code}' ",
                                               "AND route='{this_route_no}' ",
                                               "AND direction='{tt_direction}' ",
                                               "AND last_date>='{format(min_datetime, '%Y-%m-%d')}' ",
                                               "AND SUBSTRING(dow FROM {min_dow} FOR 1)='1' ",
                                               "AND (journey_scheduled BETWEEN '{format(min_datetime, '%H:%M:%S')}' AND '{format(max_datetime, '%H:%M:%S')}') ",
                                               ";")))
      
      #only go if it returns a result
      if(nrow(tt_testers)>0){
        
        #get only the latest possible timetable
        #in case of overlap
        #create a proportion of journey column
        #tt_testers$journey_proportion <- tt_testers$n/max(tt_testers$n)
        tt_testers <- tt_testers[start_date==max(start_date),
                                 ][,journey_proportion:=n/max(n), by=.(id)
                                   ][arrive>"12:00:00", time:=as.POSIXct(paste0(strftime(min_datetime, format = "%Y-%m-%d")," ",arrive))
                                     ][arrive<"12:00:00", time:=as.POSIXct(paste0(strftime(max_datetime, format = "%Y-%m-%d")," ",arrive))]
  
        #get lm details for the current journey
        tran_start_time <- one_journey[,.(journey_proportion, journey_number, time=transaction_datetime),
                                       ][,.(time_intercept=as.POSIXct((abs(lm(journey_proportion ~ time)$coefficients[1])/lm(journey_proportion ~ time)$coefficients[2]), origin = "1970-01-01")), 
                                         by=journey_number
                                         ][1, time_intercept]
        
        #get lms for all the tt journeys
        tt_nearest_id <- tt_testers[, .(journey_proportion, journey_scheduled, time, tt_id=id),
                                    ][,.(time_intercept=as.POSIXct((abs(lm(journey_proportion ~ time)$coefficients[1])/lm(journey_proportion ~ time)$coefficients[2]), origin = "1970-01-01")), by=.(journey_scheduled, tt_id)
                                      ][,start_diff:=abs(tran_start_time-time_intercept)
                                        ][order(start_diff)
                                          ][1, tt_id]
        
        
        #subset the tt_data to get only the current journey's data
        #round up journey proportions to one decimal place and join
        tt_one <- tt_testers[id==tt_nearest_id,
                             ][,.(time, naptan_code, round_journey_prop=round(journey_proportion, 1))]
        
        #joining variables right
        one_journey <- one_journey[,.(record_id, time=transaction_datetime, round_journey_prop=round(journey_proportion, 1))]
        
        #make the joins and get arrays of possible stop codes
        tt_prop_stop_array <- tt_one[one_journey, on="round_journey_prop", roll="nearest", allow.cartesian=TRUE
                                     ][,.(prop_join_stops=paste0("{'",paste(naptan_code, collapse = "', '"),"'}")),by=record_id]
        
        tt_time_stop_array <- tt_one[one_journey, on="time", roll="nearest", allow.cartesian=TRUE
                                     ][, .(time_join_stops=paste0("{'",paste(naptan_code, collapse = "', '"),"'}")),by=record_id]
        
        #join these together
        tt_array_comb <- tt_time_stop_array[tt_prop_stop_array, on="record_id"
                                            ][,tt_id:=tt_nearest_id]
        
        #then upload a table of record_id and time stops and proportion stops
        dbWriteTable(con, c(dummy_data_schema, record_vs_codes), tt_array_comb, row.names=FALSE, append=TRUE)
        
        }
      
    
      }
  
  }

  
  }
)


