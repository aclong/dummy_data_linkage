#gonna try out a version of the linking algorithm from the lab


#can you do a version of it with the timetable data?

library(stringi)
library(stringr)
library(RPostgreSQL)
library(data.table)
library(ggplot2)
library(lubridate)
library(glue)
library(hrbrthemes)
library(scales)

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

#set year and month

year_p <- "2015"
month_p <- "oct"

my_date <- "2015-10-13"

#what bus route?
bus_route <- 97
operator_code <- "TNXB"

time_one <- "09:00:00"
time_two <- "19:05:00"

#get out one bus service for the given day
route_day_tt <- dbGetQuery(con, glue("SELECT * FROM timetables.tt_all WHERE operator='{operator_code}' AND route='{bus_route}' AND start_date<'{my_date}' AND last_date>'{my_date}' AND dow='1111100' AND (journey_scheduled BETWEEN '{time_one}' AND '{time_two}') ORDER BY journey_scheduled;"))

head(route_day_tt)

route_day_tt <- setDT(route_day_tt)

#make the time into a different thing
route_day_tt$tran_time <- as.POSIXct(route_day_tt$arrive, format="%H:%M:%S")

#how many different overlapping timetables?
print(paste0(length(unique(route_day_tt$start_date)), " different route start dates"))

print(paste0(length(unique(route_day_tt$id)), " different route ids"))

print(paste0(length(unique(route_day_tt$journey_scheduled)), " different journey start times"))

#look at the departure times
#journey is about 50 min at midday
unique(route_day_tt$journey_scheduled)

#ids and directions
ids_dirs <- route_day_tt[,.(id, direction),.(id, direction)]

#subset by the later one
route_day_tt <- route_day_tt[start_date=="2015-04-26",][order(journey_scheduled)]

#get a list of times that probably make up different directions

list_of_dep_times <- c("12:00:00", "12:54:00", "13:45:00", "14:36:00", "15:30:00")

sub_route_day_tt <- route_day_tt[journey_scheduled %in% list_of_dep_times,]

#lets make another subset
sec_list_of_deps <- c("09:01:00", "10:10:00", "11:05:00", "12:06:00", "13:01:00", "13:55:00")

sec_sub_route_day_tt <- route_day_tt[journey_scheduled %in% sec_list_of_deps,]

sub_route_day_tt <- sec_sub_route_day_tt

head(sub_route_day_tt)

unique(sub_route_day_tt$direction)

#have a look
sub_route_day_tt[,.(journey_scheduled, direction, last_stop = max(tran_time)),.(journey_scheduled, direction)]
#sucessful fake data!!!

#second time around the ins and outs didn't work so well
#so different version of the reving is needed



sub_route_day_tt <- sub_route_day_tt[order(tran_time)]
head(sub_route_day_tt)

#make fake fare stages
sub_route_day_tt$fake_stage <- round(sub_route_day_tt$n/3)

for(i in 1:length(sec_list_of_deps)){
  if(i%%2==0){
    rev_stages <- rev(sub_route_day_tt$fake_stage[sub_route_day_tt$journey_scheduled==sec_list_of_deps[i]])
    
    sub_route_day_tt$fake_stage[sub_route_day_tt$journey_scheduled==sec_list_of_deps[i]] <- rev_stages
  }
}

#plot with the current data
bus_dir_plot_one <- ggplot(sub_route_day_tt, aes(x=tran_time, y=fake_stage, col=journey_scheduled, shape=direction)) +
  theme_ipsum_rc() +
  geom_point() +
  scale_colour_ft(name = "Journey No.")# +
  #scale_shape_manual(name= "Bus Direction", values = c("O"=2, "I"=6, "unknown"=4))

bus_dir_plot_one


#take a sample
ransam_sub <- sub_route_day_tt[sample(.N, ceiling(.2*nrow(sub_route_day_tt)))][order(tran_time)]

#now you have your sample!!!
#now plot with reversed stages
bus_dir_plot_one <- ggplot(ransam_sub, aes(x=arrive, y=fake_stage, col=journey_scheduled, shape=direction)) +
  theme_ipsum_rc() +
  geom_point() +
  scale_colour_ft(name = "Journey No.")# +
  #scale_shape_manual(name= "Bus Direction", values = c("O"=2, "I"=6, "unknown"=4))

bus_dir_plot_one

#did this quite a few times so maybe just randomly select a lot more next time

#get max and min fake stages
min_stage <- min(sub_route_day_tt$fake_stage)

max_stage <- max(sub_route_day_tt$fake_stage)

#so, the previous threshold inputs are

abs_same_journey_window <- duration(20, units = "minutes")
same_journey_window <- duration(20, units = "minutes")


same_stage_window <- duration(3, units = "minutes")

#this is a bit samller than it would be in the real one

#reset the new direction stuff
ransam_sub$new_dir <- "blank"
ransam_sub$new_jorno <- "0"

#make sure it is ordered by time before the loop
ransam_sub <- ransam_sub[order(tran_time)]

head(ransam_sub)

#############################
###initial simple version
for(i in 1:nrow(ransam_sub)){
  
  #statement to assign direction and journey number
  if(i==1){
    direction <- "unknown"
  }else if((ransam_sub$fake_stage[i]==min_stage & ransam_sub$fake_stage[i-1]!=min_stage) 
           | (ransam_sub$fake_stage[i]==max_stage & ransam_sub$fake_stage[i-1]!=max_stage)
  ){
    direction <- "unknown"
  }else if((((ransam_sub$tran_time[i]-ransam_sub$tran_time[i-1])<=(ransam_sub$tran_time[i+1]-ransam_sub$tran_time[i])) 
            & ((ransam_sub$fake_stage[i]-ransam_sub$fake_stage[i-1])>0) 
            & (ransam_sub$tran_time[i]<(ransam_sub$tran_time[i-1]+same_journey_window)))
  ){
    direction <- "out"
  }else if((((ransam_sub$tran_time[i]-ransam_sub$tran_time[i-1])<=(ransam_sub$tran_time[i+1]-ransam_sub$tran_time[i])) 
            & ((ransam_sub$fake_stage[i]-ransam_sub$fake_stage[i-1])<0)
            & (ransam_sub$tran_time[i]<(ransam_sub$tran_time[i-1]+same_journey_window)))
  ){
    direction <- "in"
  }else if(((ransam_sub$fake_stage[i]==ransam_sub$fake_stage[i-1]) & (ransam_sub$tran_time[i]<(ransam_sub$tran_time[i-1]+same_stage_window)))
  ){
    direction <- direction
  }
  
  #assign direction
  ransam_sub$new_dir[i] <- direction
  
  #if direction is different from previous one add 1 to journey counter
  if(i==1){
    journey_counter <- 1
  }else if(ransam_sub$new_dir[i]!=ransam_sub$new_dir[i-1]){
    journey_counter <- journey_counter+1
  }
  #then statement to add these to the new columns
  ransam_sub$new_jorno[i] <- journey_counter
  
}


###########################
##new version taking into account the next transaction as well

for(i in 1:nrow(ransam_sub)){
  
  print(i)
  #statement to assign direction and journey number
  if((i==1)
     && ((((ransam_sub$fake_stage[i+1]-ransam_sub$fake_stage[i])>0) 
          && (ransam_sub$tran_time[i+1]<(ransam_sub$tran_time[i]+same_journey_window)))
         | ((ransam_sub$fake_stage[i+1]==ransam_sub$fake_stage[i]) 
            && ((ransam_sub$fake_stage[i+2]-ransam_sub$fake_stage[i])>0) 
            && (ransam_sub$tran_time[i+2]<(ransam_sub$tran_time[i]+same_journey_window)))
         | ((ransam_sub$fake_stage[i+1]==ransam_sub$fake_stage[i])
            && (ransam_sub$fake_stage[i+2]==ransam_sub$fake_stage[i])
            && ((ransam_sub$fake_stage[i+3]-ransam_sub$fake_stage[i])>0) 
            && (ransam_sub$tran_time[i+3]<(ransam_sub$tran_time[i]+same_journey_window))))
     ){
    #checks whether any of the next three points is increasing and within journey window
    #if so assigns out
    
    direction<- "out"
  }else if((i==1)
           && ((((ransam_sub$fake_stage[i+1]-ransam_sub$fake_stage[i])<0) 
                && (ransam_sub$tran_time[i+1]<(ransam_sub$tran_time[i]+same_journey_window)))
               | ((ransam_sub$fake_stage[i+1]==ransam_sub$fake_stage[i]) 
                  && ((ransam_sub$fake_stage[i+2]-ransam_sub$fake_stage[i])<0) 
                  && (ransam_sub$tran_time[i+2]<(ransam_sub$tran_time[i]+same_journey_window)))
               | ((ransam_sub$fake_stage[i+1]==ransam_sub$fake_stage[i])
                  && (ransam_sub$fake_stage[i+2]==ransam_sub$fake_stage[i])
                  && ((ransam_sub$fake_stage[i+3]-ransam_sub$fake_stage[i])<0) 
                  && (ransam_sub$tran_time[i+3]<(ransam_sub$tran_time[i]+same_journey_window))))){
    direction <- "in"
    
    #like above but for decreasing
    
  }else if((i==1) 
           && (ransam_sub$tran_time[i+1]>(ransam_sub$tran_time[i]+same_journey_window))
           && (ransam_sub$tran_time[i]>(ransam_sub$tran_time[i-1]+same_journey_window))){
    #checks if the transaction if too far from any other transaction to be compared
    
    direction <- "unknown"
  }else if((ransam_sub$fake_stage[i]==min_stage && ransam_sub$fake_stage[i-1]!=min_stage) 
           | (ransam_sub$fake_stage[i]==max_stage && ransam_sub$fake_stage[i-1]!=max_stage)
  ){
    #if the transaction is at the minimum or maximum farestage
    # don't know why the second part of this is in there about cheking that the previous point is not min/max stage
    direction <- "unknown"
  }else if(ransam_sub$tran_time[i]==ransam_sub$tran_time[i-1]){
    #sanity check for a time error in sample set 
    #(transaction time rounding led to a lower fare stage being assinged the same time as a higher one or vice versa)
    direction <- direction
  }else if(((((ransam_sub$tran_time[i]-ransam_sub$tran_time[i-1])<(ransam_sub$tran_time[i+1]-ransam_sub$tran_time[i])) | (i==nrow(ransam_sub))) 
           && ((ransam_sub$fake_stage[i]-ransam_sub$fake_stage[i-1])>0) 
           && (ransam_sub$tran_time[i]<(ransam_sub$tran_time[i-1]+same_journey_window))
           && (ransam_sub$fake_stage[i]>1))
           | ((((ransam_sub$tran_time[i]-ransam_sub$tran_time[i-1])>(ransam_sub$tran_time[i+1]-ransam_sub$tran_time[i]))) 
              && (i<nrow(ransam_sub))
              && ((ransam_sub$fake_stage[i+1]-ransam_sub$fake_stage[i])>0) 
              && (ransam_sub$tran_time[i+1]<(ransam_sub$tran_time[i]+same_journey_window))) 
           | ((((ransam_sub$tran_time[i]-ransam_sub$tran_time[i-1])>(ransam_sub$tran_time[i+2]-ransam_sub$tran_time[i]))) 
              && (i<(nrow(ransam_sub)-1))
              && (ransam_sub$fake_stage[i+1]==ransam_sub$fake_stage[i])
              && ((ransam_sub$fake_stage[i+2]-ransam_sub$fake_stage[i])>0) 
              && (ransam_sub$tran_time[i+2]<(ransam_sub$tran_time[i]+same_journey_window))) 
           | ((((ransam_sub$tran_time[i]-ransam_sub$tran_time[i-1])>(ransam_sub$tran_time[i+3]-ransam_sub$tran_time[i])))
              && (i<(nrow(ransam_sub)-2))
              && (ransam_sub$fake_stage[i+1]==ransam_sub$fake_stage[i])
              && (ransam_sub$fake_stage[i+2]==ransam_sub$fake_stage[i])
              && ((ransam_sub$fake_stage[i+3]-ransam_sub$fake_stage[i])>0) 
              && (ransam_sub$tran_time[i+3]<(ransam_sub$tran_time[i]+same_journey_window)))
           | ((ransam_sub$fake_stage[i]!=min_stage) 
              && (ransam_sub$fake_stage[i-1]==min_stage) 
              && (ransam_sub$tran_time[i]<(ransam_sub$tran_time[i-1]+same_journey_window)))
           ){
    #see if the direction is out
    #compares against the point before and the 3 following points 
    #for relative positions in the fare stage
    #turning this into a data.table commange would sort out the iterability of this
    direction <- "out"
  }else if(((((ransam_sub$tran_time[i]-ransam_sub$tran_time[i-1])<(ransam_sub$tran_time[i+1]-ransam_sub$tran_time[i])) | i==nrow(ransam_sub)) 
           && ((ransam_sub$fake_stage[i]-ransam_sub$fake_stage[i-1])<0)
           && (ransam_sub$tran_time[i]<(ransam_sub$tran_time[i-1]+same_journey_window))) 
           | ((((ransam_sub$tran_time[i]-ransam_sub$tran_time[i-1])>(ransam_sub$tran_time[i+1]-ransam_sub$tran_time[i]))) 
              && (i<nrow(ransam_sub)) 
              && ((ransam_sub$fake_stage[i+1]-ransam_sub$fake_stage[i])<0) 
              && (ransam_sub$tran_time[i+1]<(ransam_sub$tran_time[i]+same_journey_window)))
           | ((((ransam_sub$tran_time[i]-ransam_sub$tran_time[i-1])>(ransam_sub$tran_time[i+2]-ransam_sub$tran_time[i])))
              && (i<(nrow(ransam_sub)-1))
              && (ransam_sub$fake_stage[i+1]==ransam_sub$fake_stage[i])
              && ((ransam_sub$fake_stage[i+2]-ransam_sub$fake_stage[i])<0) 
              && (ransam_sub$tran_time[i+2]<(ransam_sub$tran_time[i]+same_journey_window)))
           | ((((ransam_sub$tran_time[i]-ransam_sub$tran_time[i-1])>(ransam_sub$tran_time[i+3]-ransam_sub$tran_time[i]))) 
              && (i<(nrow(ransam_sub)-2))
              && (ransam_sub$fake_stage[i+1]==ransam_sub$fake_stage[i])
              && (ransam_sub$fake_stage[i+2]==ransam_sub$fake_stage[i])
              && ((ransam_sub$fake_stage[i+3]-ransam_sub$fake_stage[i])<0) 
              && (ransam_sub$tran_time[i+3]<(ransam_sub$tran_time[i]+same_journey_window)))
           | ((ransam_sub$fake_stage[i]!=max_stage) 
              && (ransam_sub$fake_stage[i-1]==max_stage) 
              && (ransam_sub$tran_time[i]<(ransam_sub$tran_time[i-1]+same_journey_window)))
           ){
    #like above but for fare stages going down
    
    direction <- "in"
  }else if((ransam_sub$fake_stage[i]==ransam_sub$fake_stage[i-1])
            && (ransam_sub$tran_time[i]>(ransam_sub$tran_time[i-1]+same_stage_window))
            && (ransam_sub$tran_time[i+1]>(ransam_sub$tran_time[i]+same_stage_window))
           ){
    #check that the transaction is within reasonable distance of the previous one
    #if not assign unkown
    
    direction <- "unknown"
  }else if(((ransam_sub$fake_stage[i]==ransam_sub$fake_stage[i-1]) 
            && (ransam_sub$tran_time[i]<(ransam_sub$tran_time[i-1]+same_stage_window)))
           ){
    #if the transaction is within the stage window of the previous one and 
    #none of the other transactions relate to it then assign same as previous
    
    direction <- direction
  }
  
  #assign direction
  ransam_sub$new_dir[i] <- direction
  
  #if direction is different from previous one add 1 to journey counter
  if(i==1){
    journey_counter <- 1
  }else if(ransam_sub$new_dir[i]!=ransam_sub$new_dir[i-1]){
    journey_counter <- journey_counter+1
  }
  #then statement to add these to the new columns
  ransam_sub$new_jorno[i] <- journey_counter
  
}

#need more colours
#get more colours out of palette
colour_count <- length(unique(ransam_sub$new_jorno))+1
ft_ramp_cols= colorRampPalette(ft_pal()(8))(colour_count)


#plot the results
bus_dir_plot_one <- ggplot(ransam_sub, aes(x=tran_time, y=fake_stage, col=new_jorno, shape=new_dir)) +
  theme_ipsum_rc() +
  geom_point() +
  scale_colour_manual(name = "Journey No.", values = ft_ramp_cols) +
  scale_shape_manual(name= "Bus Direction", values = c("out"=2, "in"=6, "unknown"=4))

bus_dir_plot_one

#now translate into a DT solution

class(ransam_sub)

#keep only necessary columns
dt_sample <- ransam_sub[, .(operator, route, tran_time, fake_stage),]

head(dt_sample)

setkey(dt_sample, operator, tran_time)

dt_test <- dt_sample[, new_dir:=ifelse(((tran_time<shift(tran_time+same_journey_window, 1)) 
                                       && (fake_stage<shift(fake_stage,1)) 
                                       && (tran_time-shift(tran_time,1))<(shift(tran_time, 1, type="lead")-tran_time)
                                       ) | ((tran_time+same_journey_window)<shift(tran_time, 1, type = "lead")
                                            &&(shift(tran_time, 1)-tran_time)>(shift(tran_time, 1, type="lead")-tran_time)
                                            && (fake_stage>shift(fake_stage,1, type = "lead"))), 
                                       "in", ifelse() ), by=.(operator, route)]

###############################
#SQL version
##rather than data.table it would be best to turn it into an SQL query

# here is an example of conditional ordering/formatting
#https://stackoverflow.com/questions/50599371/sql-server-calculate-cumulative-sum-conditional-running-total-depending-on-dif

#also this one looks usefull
#https://stackoverflow.com/questions/51954221/increment-column-for-streaks/51954348#51954348

#and this
#https://stackoverflow.com/questions/18987791/how-do-i-efficiently-select-the-previous-non-null-value

#getting closer to it with btree/knn workings
#https://www.postgresql.org/message-id/flat/ce35e97b-cf34-3f5d-6b99-2c25bae49999@postgrespro.ru#ce35e97b-cf34-3f5d-6b99-2c25bae49999@postgrespro.ru

#let's write a db version of the dummy data

#turn fare stage back to numeric before
test_one$fare_stage <- as.integer(test_one$fare_stage)

#dbWriteTable(con, c("dummy_trans_tt", "test_one_ran_samp"), value = test_one, row.names=FALSE, overwrite=TRUE)

#now that the table is created try the SQL version

#make some string versions of the same journey window

string_journey_window <- "00:20:00"
string_stage_window <- "00:03:00"

#what about calculating same journey window on the average time from 
#maximum to minimum stage for the given time of day for that dow

#calculate how long on average it is from max to min for that 
#bus at that time of day

#how to do it

min_stage <- dbGetQuery(con, glue("SELECT MIN(fare_stage) FROM dummy_trans_tt.test_one_ran_samp;"))
max_stage <- dbGetQuery(con, glue("SELECT MAX(fare_stage) FROM dummy_trans_tt.test_one_ran_samp;"))

#now us them in another query

sanity_check_time <-3

#will need another set of parameters for this window

avg_journey_time <- dbGetQuery(con, glue("SELECT ends.fare_stage, ",
                                         "CASE WHEN ends.fare_stage={min_stage} THEN 'in' WHEN ends.fare_stage={max_stage} THEN 'out' END AS direction, ",
                                         "ends.tran_time-(LAG(ends.tran_time, 1) OVER (PARTITION BY ends.operator, ends.route ORDER BY ends.tran_time)) AS journey_time, ",
                                         "DATE_PART('hour', ends.tran_time) AS hour_of_day, ",
                                         "DATE_PART('dow', ends.tran_time) AS day_of_week FROM ",
                                         "(SELECT *, (LAG(fare_stage, 1) OVER (PARTITION BY operator, route ORDER BY tran_time)) AS prev_stage FROM dummy_trans_tt.test_one_ran_samp ",
                                         " WHERE (fare_stage={min_stage} OR fare_stage={max_stage}) ",
                                         " ORDER BY tran_time) ends ",
                                         " WHERE ends.fare_stage!=ends.prev_stage;"))

average_journey_time

int_journey_window <- 20
int_stage_window <- 5

#what about using coalesce

over_strings <- c("operator", "route")

tran_string <- "tran_time"

sql_test <- dbGetQuery(con, glue("SELECT *, ",
                                 "CASE WHEN ",
                                 "dir1.new_direction IS NOT NULL THEN dir1.new_direction ",
                                 "WHEN dir1.new_direction IS NULL AND (dir1.fare_stage=(LAG(dir1.fare_stage, 1) OVER dir1w)) AND ((dir1.tran_time-(LAG(dir1.tran_time, 1) OVER dir1w)<INTERVAL '{int_stage_window} minutes')) THEN (LAG(dir1.new_direction, 1) OVER dir1w) ",
                                 " END AS direction ",
                                 "FROM ",
                                 "(SELECT *, ", 
                                 "CASE WHEN ( ",
                                 "((fare_stage>LAG(fare_stage, 1) OVER w) AND (tran_time-(LAG(tran_time, 1) OVER w)<INTERVAL '{int_journey_window} minutes') AND ((tran_time-(LAG(tran_time, 1) OVER w)<=((LEAD(tran_time, 1) OVER w)-tran_time)) OR (fare_stage=LEAD(fare_stage,1) OVER w) OR ((LEAD(fare_stage,1) OVER w) IS NULL)) ) ",
                                 "OR ((fare_stage<(LEAD(fare_stage, 1) OVER w)) AND ((LEAD(tran_time, 1) OVER w)-tran_time<INTERVAL '{int_journey_window} minutes') AND (((tran_time-(LAG(tran_time, 1) OVER w)>(LEAD(tran_time, 1) OVER w)-tran_time)) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w))) ) ",
                                 "OR ((fare_stage<(LEAD(fare_stage, 2) OVER w)) AND (fare_stage=(LEAD(fare_stage, 1) OVER w)) AND ((LEAD(tran_time, 2) OVER w)-tran_time<INTERVAL '{int_journey_window} minutes') AND (((tran_time-(LAG(tran_time, 1) OVER w))>((LEAD(tran_time, 2) OVER w)-tran_time)) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w))) ) ",
                                 "OR ((fare_stage<(LEAD(fare_stage, 3) OVER w)) AND (fare_stage=(LEAD(fare_stage, 1) OVER w)) AND (fare_stage=(LEAD(fare_stage, 2) OVER w)) AND ((LEAD(tran_time, 3) OVER w)-tran_time<INTERVAL '{int_journey_window} minutes') AND (((tran_time-(LAG(tran_time, 1) OVER w))>((LEAD(tran_time, 3)  OVER w)-tran_time)) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w))) ) ",
                                 ") THEN 'out' ",
                                 "WHEN (",
                                 "((fare_stage<LAG(fare_stage, 1) OVER w) AND (tran_time-(LAG(tran_time, 1) OVER w)<INTERVAL '{int_journey_window} minutes') AND ((tran_time-(LAG(tran_time, 1) OVER w)<=((LEAD(tran_time, 1) OVER w)-tran_time)) OR (fare_stage=LEAD(fare_stage,1) OVER w) OR ((LEAD(fare_stage,1) OVER w) IS NULL)) ) ",
                                 "OR ((fare_stage>(LEAD(fare_stage, 1) OVER w)) AND ((LEAD(tran_time, 1) OVER w)-tran_time<INTERVAL '{int_journey_window} minutes') AND (((tran_time-(LAG(tran_time, 1) OVER w)>(LEAD(tran_time, 1) OVER w)-tran_time)) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w))) ) ",
                                 "OR ((fare_stage>(LEAD(fare_stage, 2) OVER w)) AND (fare_stage=(LEAD(fare_stage, 1) OVER w)) AND ((LEAD(tran_time, 2) OVER w)-tran_time<INTERVAL '{int_journey_window} minutes') AND (((tran_time-(LAG(tran_time, 1) OVER w))>((LEAD(tran_time, 2) OVER w)-tran_time)) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w))) ) ",
                                 "OR ((fare_stage>(LEAD(fare_stage, 3) OVER w)) AND (fare_stage=(LEAD(fare_stage, 1) OVER w)) AND (fare_stage=(LEAD(fare_stage, 2) OVER w)) AND ((LEAD(tran_time, 3) OVER w)-tran_time<INTERVAL '{int_journey_window} minutes') AND (((tran_time-(LAG(tran_time, 1) OVER w))>((LEAD(tran_time, 3) OVER w)-tran_time)) OR (LAG(fare_stage, 1) OVER w IS NULL) OR (fare_stage=(LAG(fare_stage, 1) OVER w)) ) ) ",
                                 ") THEN 'in' ",
                                 "END AS new_direction ",
                                 "FROM dummy_trans_tt.test_one_ran_samp ",
                                 "WINDOW w AS (PARTITION BY {over_strings[1]}, {over_strings[2]} ORDER BY {order_string}) ",
                                 ") dir1 ",
                                 "WINDOW dir1w AS (PARTITION BY dir1.{over_strings[1]}, dir1.{over_strings[2]} ORDER BY dir1.{order_string});"))

#there is still an error if a fare stage has the same tran time, but can be put in the wrong order 
#and then creates a little error
#what about changing it so that in the second run it addresses this problem?
#look at the order of transactions as they run through, so if you have 
#five in a row wihtin a give time you make another run of it

#so the answer would be to make a test that finds the previous min_stage 
#for each fare_stage, that is within a sensible journey time window, say 1 hr

#with actual transactions can use the record ID along with the transaction time 
#to get the proper order and avoid mis-ordering

#