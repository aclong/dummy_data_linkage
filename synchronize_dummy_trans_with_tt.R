#synchronize the dummy data with the timetable data to find matching journeys

#this problem seems to have been encountered in many different areas

#dna sequence matching
#http://afproject.org/
#https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2677745/

#one of these is high frequency trading
##see here: https://rdrr.io/rforge/highfrequency/man/refreshTime.html

#another is the synchrony package
#ssems to be developed from biology
##https://cran.r-project.org/web/packages/synchrony/synchrony.pdf

#problem of synchronizing two time series (i think this is more about just plotting though)
#https://stackoverflow.com/questions/41352042/synchronise-and-plot-two-timeseries-data-sets-in-r

#pretty comperehensive workththrough 
#THESE ARE THE ONES TO USE AND LOOK THROUGH
#here: https://stats.stackexchange.com/questions/31666/how-can-i-align-synchronize-two-signals
#https://stats.stackexchange.com/questions/130843/how-to-synchronize-two-signals-using-fft-in-r?rq=1

#mmeasuring synchrony: https://towardsdatascience.com/four-ways-to-quantify-synchrony-between-time-series-data-b99136c4a9c9

#for my purposes:

#have time stap for the transaction tt
#have timestamp for the stop in the tt

#to compare better you should

#get the stop/fare_stage number as a proportion of the complete journey
#this would then make them more comparable

#what are the packages i need?
library(RPostgreSQL)
library(data.table)
library(glue)
library(lubridate)

#run the connections
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

#now to load in the two datasets

#first get the "transaction data"
#do I still need the date/time counter as I do this?

#can loop through journeys for each machine ID and compare them againse the timetable

machine_ids <- dbGetQuery(con, glue("SELECT DISTINCT machine_id FROM {dummy_data_schema}.{new_table_name}_journey_no_test;"))


#there are 10k transactions in this set

one_machine_transaction_data <- dbGetQuery(con, glue("SELECT * FROM {dummy_data_schema}.{new_table_name}_journey_no_test WHERE machine_id='{machine_ids[1,1]}';"))

#current directions are a bit messed up so let's focus on only outbound journeys
#how many distinct journey ids are there in this list?
journey_nums <- unique(one_machine_transaction_data$journey_number)

#turns out there are 14
#is 100 enough for the trip counter?

#seems like it might be

#get one journey out
one_journey <- one_machine_transaction_data[one_machine_transaction_data$journey_number==journey_nums[1],]

#what date info shall I extract?
#any journey that overlaps any part of this

min_datetime <- min(one_journey$transaction_datetime)

max_datetime <- max(one_journey$transaction_datetime)

#now get all timetable journeys that overlp with this

#WHERE start_date<'{end_date}' AND last_date>'{start_date}'

dbGetQuery(con, glue("SELECT * FROM timetables.tt_all WHERE start_date<='{format(max_datetime, '%Y-%m-%d')}' AND last_date>='{format(min_datetime, '%Y-%m-%d')}' AND journey_scheduled<='{format(min_datetime, '%H:%M:%S')}' AND arrive;"))
