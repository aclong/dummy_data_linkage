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

#get all operators/routes from this period
operator_route_list <- dbGetQuery(con, glue("SELECT operator, route FROM timetables.tt_all WHERE start_date<'{end_date}' AND last_date>'{start_date}' GROUP BY operator, route ORDER BY operator, route;"))

#need unique bus numbers for each route for each day

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
    
    #now get all the results back from the tt for this date/dow
    all_journeys <- setDT(dbGetQuery(con, glue("SELECT start_date, last_date, type, journey_scheduled, id, direction, arrive, depart, n AS stop_series ",
                                         " FROM timetables.tt_all WHERE",
                                         " start_date<='{this_date}' AND last_date>='{this_date}' ",
                                         " AND SUBSTRING(dow FROM {this_dow} FOR 1)='1'",
                                         " AND operator='{this_operator}' AND route='{this_route}';")))
    
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
        
        last_arrives <- period_sub[,.(id, journey_schduled, max(arrive)),by=.(id, journey_schduled)]
        
        
      }
      
    }
    
  }
  
}
