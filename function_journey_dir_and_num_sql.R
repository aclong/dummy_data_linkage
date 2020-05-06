
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

#add the table name and schema name
dummy_data_schema <- "dummy_trans_tt"
dummy_data_table <- "oct_2015_dummy_sample"

#table to create
new_table_name <- paste0(dummy_data_table, "_linked")

#get the list of cols in the table
#column_names <- colnames(dbGetQuery(con, glue("SELECT * FROM {dummy_data_schema}.{dummy_data_table} LIMIT 1;")))


#strings for the window creating all grouoping and ordering
over_string_list <- c("operator_code", "route_no", "machine_id")

plain_over_strings <- paste(over_string_list, collapse=", ")

dir1_over_strings <- paste(paste0("dir1.",over_string_list), collapse=", ")

dir2_over_strings <- paste(paste0("dir2.",over_string_list), collapse=", ")

tran_string <- "transaction_datetime"

#now to create the window variables used
int_journey_window <- 20
int_stage_window <- 5


#the actual command
dbGetQuery(con, glue("CREATE TABLE {dummy_data_schema}.{new_table_name} AS ",
                                 "SELECT *, ",
                                 "CASE WHEN ",
                                 "dir1.new_direction IS NOT NULL THEN dir1.new_direction ",
                                 "WHEN dir1.new_direction IS NULL ",
                                 " AND (dir1.fare_stage=(LAG(dir1.fare_stage, 1) OVER dir1w)) ",
                                 " AND ((dir1.{tran_string}-(LAG(dir1.{tran_string}, 1) OVER dir1w)<INTERVAL '{int_stage_window} minutes')) ",
                                 " THEN (LAG(dir1.new_direction, 1) OVER dir1w) ",
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


#adding a version with a journey_number assigner

#create a sequence to assign the journey numbers from

dbGetQuery(con, glue("CREATE SEQUENCE journey_sequence ",
                     "INCREMENT 1",
                     "MINVALUE 1 ",
                     "MAXVALUE 100 ",
                     "START 1 ",
                     "CYCLE ;"))

dbGetQuery(con, glue("CREATE TABLE {dummy_data_schema}.{new_table_name} AS ",
                     "SELECT dir2.*, ",
                     "CASE WHEN dir2.direction!=(LAG(dir2.direction,1) OVER dir2w) ",
                     "OR (dir2.{tran_string}-LAG(dir2.{tran_string},1))>INTERVAL '{int_journey_window} minutes' ",
                     "THEN nextval(journey_sequence) ",
                     "ELSE currval(journey_sequence) END AS journey_number ", 
                     "FROM ",
                     "(SELECT *, ",
                     "CASE WHEN ",
                     "dir1.new_direction IS NOT NULL THEN dir1.new_direction ",
                     "WHEN dir1.new_direction IS NULL ",
                     " AND (dir1.fare_stage=(LAG(dir1.fare_stage, 1) OVER dir1w)) ",
                     " AND ((dir1.{tran_string}-(LAG(dir1.{tran_string}, 1) OVER dir1w)<INTERVAL '{int_stage_window} minutes')) ",
                     " THEN (LAG(dir1.new_direction, 1) OVER dir1w) ",
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
                     "WINDOW dir1w AS (PARTITION BY {dir1_over_strings} ORDER BY dir1.{tran_string})) dir2 ",
                     "WINDON dir2w AS (PARTITION BY {dir2_over_strings} ORDER BY dir2.{tran_string});"))



#make some indexes on it
dbGetQuery(con, glue("CREATE INDEX ON {dummy_data_schema}.{new_table_name} (operator_code);"))
dbGetQuery(con, glue("CREATE INDEX ON {dummy_data_schema}.{new_table_name} (route_no);"))
dbGetQuery(con, glue("CREATE INDEX ON {dummy_data_schema}.{new_table_name} (machine_id);"))

dbGetQuery(con, glue("VACUUM ANALYZE {dummy_data_schema}.{new_table_name};"))

##################
#add a unique id column
dbGetQuery(con, glue("ALTER TABLE {dummy_data_schema}.{new_table_name} ",
                     "ADD COLUMN record_id serial not null primary key;"))

#make another column which stores how far through the fare/stage sequence the transaction is
#so this would be max(fare_stage)/fare_stage for each operator, route, direction

#by doing by day/route/operator/direction/day and not using machine numbers 
#you make sure to get the full range of fare stages possible


proportion_window <- c(glue("date_trunc('day', {tran_string})"),"operator_code", "route_no", "direction")

proportion_w_string <- paste(proportion_window, collapse=", ")

#write the query to get this new column of the proportion

#create column
dbGetQuery(con, glue("ALTER TABLE {dummy_data_schema}.{new_table_name} ",
                     "DROP COLUMN journey_proportion;"))

#fill column
dbGetQuery(con, glue("ALTER TABLE {dummy_data_schema}.{new_table_name} ",
                     "ADD COLUMN journey_proportion float;"))

#try a version on just one day and operator to see if it actually works

#add new column to whole table version
Sys.time()
system.time(
dbGetQuery(con, glue("UPDATE {dummy_data_schema}.{new_table_name} old_tab ",
                     "SET journey_proportion=(new_tab.fare_stage_proportion) ",
                     "FROM (SELECT record_id, CASE WHEN direction='out' AND (max(fare_stage) OVER w)!=0 THEN fare_stage::float/(max(fare_stage) OVER w)::float ",
                     "WHEN direction='in' AND (max(fare_stage) OVER w)!=0 THEN (((max(fare_stage) OVER w)::float-fare_stage::float)/(max(fare_stage) OVER w)::float) ",
                     "ELSE 'NaN' END AS fare_stage_proportion FROM {dummy_data_schema}.{new_table_name} ",
                     "WINDOW w AS (PARTITION BY {proportion_w_string})) new_tab WHERE new_tab.record_id=old_tab.record_id;"))
)
Sys.time()

#took ~7 minutes