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

