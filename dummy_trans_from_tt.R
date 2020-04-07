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

#now start the big loop
