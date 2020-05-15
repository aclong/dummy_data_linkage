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
dbGetQuery(con, glue("CREATE TABLE {dummy_data_schema}.{record_vs_codes} (record_id int, time_join_stops TEXT [], prop_join_stops TEXT []);"))




#start a for loop to go through all the machines

#the data is all between september and november 2015 so no need to sample

#stipulate only the varaiables you need
trans_vars_string <- "operator_code, route_no, transaction_datetime, direction, journey_number, record_id, journey_proportion"

tt_vars_string <- " start_date, journey_scheduled, id, direction, naptan_code, n, arrive, dow"



#get all the machine IDs
all_mach_ids <- dbGetQuery(con, glue("SELECT DISTINCT machine_id FROM {dummy_data_schema}.{new_table_name}_journey_no;"))

#length(all_mach_ids$machine_id)

for(id_num in 1:30){
  
  if(id_num%%1000==0){
    print(glue("Working on machine number {id_num} at {Sys.time()} "))
  }
  
  print(glue("Working on machine number {id_num} at {Sys.time()} "))

  #takes a machine id
  machine_id <- all_mach_ids$machine_id[id_num]

  #gets all transactions for that data
  one_machine_transaction_data <- dbGetQuery(con, glue("SELECT {trans_vars_string} FROM {dummy_data_schema}.{new_table_name}_journey_no ",
                                                       " WHERE machine_id='{machine_id}' ", 
                                                       "AND direction!='NA' ",
                                                       " ORDER BY {tran_string};"))
  
  #breaks down the data on that machine by journey number
  
  #how many distinct journey nums are there in this list?
  journey_nums <- unique(one_machine_transaction_data$journey_number)
  
  if(id_num%%1000==0){
    print(glue("It has {length(journey_nums)} journeys"))
  }
  
  print(glue("It has {length(journey_nums)} journeys"))
  
  #then go through all the journeys
  for(i in 1:length(journey_nums)){
    
    #get only one journey
    one_journey <- setDT(one_machine_transaction_data[one_machine_transaction_data$journey_number==journey_nums[i],])
    
    #check that there is only one journey in this section
    #might not need this bit if I sort out the journey number assigning cycle
    
    print(glue("journey {i}"))
    
    #get min and max times for the journey starting
    min_datetime <- min(one_journey$transaction_datetime)-hours(1)
    
    max_datetime <- max(one_journey$transaction_datetime)+hours(1)
    
    #in case the journey breaks over differen weekdays
    #get dow info
    min_dow <- wday(min_datetime, week_start = getOption("lubridate.week.start", 1))
    
    max_dow <- wday(max_datetime, week_start = getOption("lubridate.week.start", 1))
    
    #route and operator info
    operator_code <- one_journey$operator_code[1]
    route_no <- one_journey$route_no[1]
    
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
                                             "AND operator='{operator_code}' ",
                                             "AND route='{route_no}' ",
                                             "AND direction='{tt_direction}' ",
                                             "AND last_date>='{format(min_datetime, '%Y-%m-%d')}' ",
                                             "AND SUBSTRING(dow FROM {min_dow} FOR 1)='1' ",
                                             "AND (journey_scheduled BETWEEN '{format(min_datetime, '%H:%M:%S')}' AND '{format(max_datetime, '%H:%M:%S')}') ",
                                             ";")))
    
    #only go if it returns a result
    if(length(tt_testers)>0){
      
      #get only the latest possible timetable
      #in case of overlap
      tt_testers <- tt_testers[start_date==max(start_date),]
      
      #create a proportion of journey column
      #tt_testers$journey_proportion <- tt_testers$n/max(tt_testers$n)
      tt_testers <- tt_testers[,journey_proportion:=n/max(n), b=.(id)]
      
      #there is a transaction datetime issue that you have to think about
      #what happens at midnight? is the timetable journey on "sunday"'s one or is it still 
      #on saturday's and just noted differently?
      

      tt_testers <- tt_testers[arrive>"12:00:00", 
                               time:=as.POSIXct(paste0(strftime(min_datetime, format = "%Y-%m-%d")," ",arrive))
                               ][arrive<"12:00:00", 
                                 time:=as.POSIXct(paste0(strftime(max_datetime, format = "%Y-%m-%d")," ",arrive))]
        
      
      
      
      #get lm details for the current journey
      tran_lms <- one_journey[,.(journey_proportion, journey_number, time=transaction_datetime),
                              ][,.(time_intercept=as.POSIXct((abs(lm(journey_proportion ~ time)$coefficients[1])/lm(journey_proportion ~ time)$coefficients[2]), origin = "1970-01-01")), 
                              by=journey_number]
      
      #get the x (time) intercept val
      tran_start_time <- tran_lms$time_intercept[1]
      
      #get lms for all the tt journeys
      tt_lms <- tt_testers[, .(journey_proportion, 
                               journey_scheduled, 
                               time,
                               tt_id=id),
                           ][,.(r_sq=summary(lm(journey_proportion ~ time))$r.squared,
                                              time_coef=lm(journey_proportion ~ time)$coefficients[2],
                                              time_intercept=as.POSIXct((abs(lm(journey_proportion ~ time)$coefficients[1])/lm(journey_proportion ~ time)$coefficients[2]), origin = "1970-01-01")), 
                             by=.(journey_scheduled, tt_id)
                             ][,start_diff:=abs(tran_start_time-time_intercept)
                               ][order(start_diff)]
      
      #get the tt_id for the value
      tt_nearest_id <- tt_lms$tt_id[1]
      
      #attache tt_id to the one_journey dt
      one_journey$tt_id <- tt_nearest_id
      
      #what about using a rolling join for it
      #mixture of time and proportion of the journey done
      #subset the tt_data to get only the current journey's data
      tt_one <- tt_testers[id==tt_nearest_id,]
      
      
      #round up journey proportions to one decimal place and join
      tt_one[,round_journey_prop:=round(journey_proportion, 1)]
      one_journey[,round_journey_prop:=round(journey_proportion, 1)]
      
      tt_prop_join <- tt_one[one_journey, on="round_journey_prop", roll="nearest", allow.cartesian=TRUE]
      
      #do another on the time of the journey
      #do another one on time
      tt_one[,time:=as.POSIXct(paste0(strftime(min_datetime, format = "%Y-%m-%d")," ",arrive))]
      
      one_journey[,time:=transaction_datetime]
      
      tt_time_join <- tt_one[one_journey, on="time", roll="nearest", allow.cartesian=TRUE]
      
      #get arrays of possible stop codes
      tt_time_stop_array <- tt_time_join[, .(time_join_stops=paste0("{'",paste(naptan_code, collapse = "', '"),"'}")),by=record_id]
      
      tt_prop_stop_array <- tt_prop_join[,.(prop_join_stops=paste0("{'",paste(naptan_code, collapse = "', '"),"'}")),by=record_id]
      
      #join these together
      
      tt_array_comb <- tt_time_stop_array[tt_prop_stop_array, on="record_id"]
      
      #then upload a table of record_id and time stops and proportion stops
      dbWriteTable(con, c(dummy_data_schema, record_vs_codes), tt_array_comb, row.names=FALSE, append=TRUE)
      
      }
    
    }

  }
