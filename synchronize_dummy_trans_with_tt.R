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

#add the table name and schema name
dummy_data_schema <- "dummy_trans_tt"
dummy_data_table <- "oct_2015_dummy_sample"

#table to create
new_table_name <- paste0(dummy_data_table, "_linked")

#transaction var name
tran_string <- "transaction_datetime"

#now to load in the two datasets

#first get the "transaction data"
#do I still need the date/time counter as I do this?

#can loop through journeys for each machine ID and compare them againse the timetable

machine_ids <- dbGetQuery(con, glue("SELECT DISTINCT machine_id FROM {dummy_data_schema}.{new_table_name}_journey_no LIMIT 200;"))


#there are 10k transactions in this set

one_machine_transaction_data <- dbGetQuery(con, glue("SELECT * FROM {dummy_data_schema}.{new_table_name}_journey_no WHERE machine_id='{machine_ids[1,1]}' ORDER BY {tran_string};"))

#current directions are a bit messed up so let's focus on only outbound journeys
#how many distinct journey ids are there in this list?
journey_nums <- unique(one_machine_transaction_data$journey_number)

#turns out there are 14
#is 100 enough for the trip counter?

#seems like it might be

#get one journey out
one_journey <- one_machine_transaction_data[one_machine_transaction_data$journey_number==journey_nums[1],]

one_journey <- setDT(one_journey)

#sopme errors showing up where the first of the "next" journey is instead assigned to the "current" one

#what date info shall I extract?
#any journey that overlaps any part of this

min_datetime <- min(one_journey$transaction_datetime)-hours(1)

max_datetime <- max(one_journey$transaction_datetime)+hours(1)

#get dow info
min_dow <- wday(min_datetime, week_start = getOption("lubridate.week.start", 1))

max_dow <- wday(max_datetime, week_start = getOption("lubridate.week.start", 1))

#route and operator info
operator_code <- one_journey$operator_code[1]
route_no <- one_journey$route_no[1]

if(one_journey$direction[1]=="in"){
  tt_direction <- "I"
}else if(one_journey$direction[1]=="out"){
  tt_direction <- "O"
}

#now get all timetable journeys that overlp with this
#might add an hour either side to make sure it's big enough

#WHERE start_date<'{end_date}' AND last_date>'{start_date}'

# could get an average of journey times from the timetbale to create the window
# for getting the testing journeys from
# 

colnames(dbGetQuery(con, glue("SELECT * FROM timetables.tt_all LIMIT 1;")))

tt_testers <- setDT(dbGetQuery(con, glue("SELECT * FROM timetables.tt_all ",
                                   "WHERE start_date<='{format(max_datetime, '%Y-%m-%d')}' ",
                                   "AND operator='{operator_code}' ",
                                   "AND route='{route_no}' ",
                                   "AND direction='{tt_direction}' ",
                                   "AND last_date>='{format(min_datetime, '%Y-%m-%d')}' ",
                                   "AND (SUBSTRING(dow FROM {min_dow} FOR 1)='1' OR SUBSTRING(dow FROM {max_dow} FOR 1)='1') ",
                                   "AND (journey_scheduled BETWEEN '{format(min_datetime, '%H:%M:%S')}' AND '{format(max_datetime, '%H:%M:%S')}') ",
                                   ";")))

#get only the latest possible timetable
tt_testers <- tt_testers[start_date==max(start_date),]

#chesk the number of journeys found

unique(tt_testers$journey_scheduled)
#there are four journeys about half hours apart starting staring at about 6 till one startinat 7 40

#create a proportion of journey column
tt_testers$journey_proportion <- tt_testers$n/max(tt_testers$n)

#do some fun scatter plots

library(hrbrthemes)
library(ggplot2)
library(RColorBrewer)


ggplot(tt_testers, aes(x=arrive, y=journey_proportion, col=journey_scheduled)) +
  theme_ipsum_rc() +
  geom_point() +
  scale_colour_ft(name = "Journey Scheduled")

ggplot(one_journey, aes(x=transaction_datetime, y=journey_proportion, col=journey_number)) +
  theme_ipsum_rc() +
  geom_point()

#make a compsite version of the tweo datasets so you can see what it looks like

tt_tester_sub <- tt_testers[, .(journey_proportion, 
                                journey_scheduled, 
                                time=as.POSIXct(paste0(strftime(min_datetime, format = "%Y-%m-%d")," ",arrive)),
                                tt_id=id),]

one_journey_sub <- one_journey[,.(journey_proportion, journey_scheduled=journey_number, time=transaction_datetime, tt_id="Transaction Data"),]

tt_tran_comp <- rbind(tt_tester_sub, one_journey_sub)


ggplot(tt_tran_comp, aes(x=time, y=journey_proportion, col=tt_id)) +
  theme_ipsum_rc() +
  geom_point()

#make a nicer version for the report

#make the journey ID

tran_vs_tt_plot <- ggplot(tt_tran_comp, aes(x=time, y=journey_proportion, col=tt_id)) +
  theme_ipsum_rc(axis_title_size = 14) +
  scale_colour_ft(name = "Journey ID") +
  geom_point(shape=24) +
  #scale_x_datetime(breaks=date_breaks(width = "1 hours"), date_labels = "%H") +
  scale_y_continuous(breaks = seq(0,1,.2)) +
  labs(x="Time", y="Journey Proportion") +
  geom_smooth(method="lm", se=FALSE)


tran_vs_tt_plot

ggsave("~/DATA/update_report_visulisations/plots/tran_vs_tt_plot_withlines.png", tran_vs_tt_plot, height = 6, width = 9)

#the journeys should always be pretty linear.

#can you think of any occasions in which they wouldn't?

#can you just find the y=0 intercept and then get the closest?

#maybe should do a common sense test of whether the relationship shown is similar

#so you get the R value then and from there go on to check which y intercept is closest

#what about skipping off the beginning and end of journeys in case they are mistaken
#^this should be taken at an earlier phase.

#time needs to be time rather than a character
#create a timestamp for the timetable data using the date extracted from the tran_time

linear_trans <- lm(formula = journey_proportion ~ time, data=tt_tran_comp[journey_scheduled=="90",])


summary(linear_trans)

#so to get the time intercept

summary(linear_trans)$r.squared

summary(linear_trans)

as.POSIXct(abs(linear_trans$coefficients[1])/linear_trans$coefficients[2], origin = "1970-01-01")

#plot the new line on top

ggplot(tt_tran_comp, aes(x=time, y=journey_proportion, col=journey_scheduled)) +
  theme_ipsum_rc() +
  geom_point() +
  geom_smooth(method="lm", size=1, se=FALSE)

#slightly different slopes even from the same data

#get the results for all the different groups
#can do this using data.table

#found here: https://stackoverflow.com/a/33754058/10087503
tt_tran_comp[,.(r_sq=summary(lm(journey_proportion ~ time))$r.squared, time_intercept=(summary(lm(journey_proportion ~ time))$r.squared)), by=journey_scheduled]

tt_tran_comp[,.(r_sq=summary(lm(journey_proportion ~ time))$r.squared,
                time_coef=lm(journey_proportion ~ time)$coefficients[2],
                time_intercept=as.POSIXct((abs(lm(journey_proportion ~ time)$coefficients[1])/lm(journey_proportion ~ time)$coefficients[2]), origin = "1970-01-01")), by=journey_scheduled]

#so now run it once for the tt and once for the trans data and compare

tran_lms <- one_journey_sub[,.(r_sq=summary(lm(journey_proportion ~ time))$r.squared,
                               time_coef=lm(journey_proportion ~ time)$coefficients[2],
                               time_intercept=as.POSIXct((abs(lm(journey_proportion ~ time)$coefficients[1])/lm(journey_proportion ~ time)$coefficients[2]), origin = "1970-01-01")), by=journey_scheduled]

tran_start_time <- tran_lms$time_intercept[1]

tt_lms <- tt_tester_sub[,.(r_sq=summary(lm(journey_proportion ~ time))$r.squared,
                           time_coef=lm(journey_proportion ~ time)$coefficients[2],
                           time_intercept=as.POSIXct((abs(lm(journey_proportion ~ time)$coefficients[1])/lm(journey_proportion ~ time)$coefficients[2]), origin = "1970-01-01")), by=journey_scheduled][,
                             start_diff:=abs(tran_start_time-time_intercept)
                           ][
                             order(start_diff)
                           ]

#look at them
#should I take 1 away from "n" in the timetable so that it starts with 0? No, that is journey proportion
#what are you actually doing here?

#make a shorter one that only get necessary info
tran_lms <- one_journey_sub[,.(time_intercept=as.POSIXct((abs(lm(journey_proportion ~ time)$coefficients[1])/lm(journey_proportion ~ time)$coefficients[2]), origin = "1970-01-01")), 
                            by=journey_scheduled]

tran_start_time <- tran_lms$time_intercept[1]

tt_lms <- tt_tester_sub[,.(r_sq=summary(lm(journey_proportion ~ time))$r.squared,
                           time_coef=lm(journey_proportion ~ time)$coefficients[2],
                           time_intercept=as.POSIXct((abs(lm(journey_proportion ~ time)$coefficients[1])/lm(journey_proportion ~ time)$coefficients[2]), origin = "1970-01-01")), 
                        by=.(journey_scheduled, tt_id)
                        ][,start_diff:=abs(tran_start_time-time_intercept)
                          ][order(start_diff)]


#nearest journey
tt_nearest_sched <- tt_lms$journey_scheduled[1]
tt_nearest_id <- tt_lms$tt_id[1]

#attache tt_id to the journey
one_journey$tt_id <- tt_nearest_id

#should I keep the ID of the journey and use this to reattache.

#maybe first step is to assign the transactions the journey ID and then can compare different 
#ways of assigning stops from there

tt_one <- tt_testers[id==tt_nearest_id,]


tt_one
one_journey

#test a join on journey proportion and another on time
#this "many" join thing may work

#what about doing the many join for time and proportion and then using 
#the group of stops as a possible answer

#round journey_prop to make vaguer
tt_one[,round_journey_prop:=round(journey_proportion, 1)]
one_journey[,round_journey_prop:=round(journey_proportion, 1)]

tt_prop_join <- tt_one[one_journey, on="round_journey_prop", roll="nearest"]

#do another one on time
tt_one[,time:=as.POSIXct(paste0(strftime(min_datetime, format = "%Y-%m-%d")," ",arrive))]

one_journey[,time:=transaction_datetime]

tt_time_join <- tt_one[one_journey, on="time", roll="nearest"]

# the time version gets more resutls as is vager
# using the journey proportion version gets fewer matches for each of the same transactions
# this measure is just as arbitrary as the time one if not more so.
# shall I add all possible matches then?

# could make a column that holds arrays of all possible stops by proportion and another by time
# 

tt_time_stop_array <- tt_time_join[, .(time_join_stops=list(naptan_code)),by=record_id]

tt_prop_stop_array <- tt_prop_join[,.(prop_join_stops=list(naptan_code)),by=record_id]

#have changed proportion to join on 1 decimal place now, have more results

#compare

tt_array_comb <- tt_time_stop_array[tt_prop_stop_array, on="record_id"]

# i think best way forward is to upload all possible matches to an array column in the db.
# can sort out later

# try a bit of mapping the different techniques and testing which is more successful


