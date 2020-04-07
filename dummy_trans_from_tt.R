#this script is to automate the simulation of trnasaction data from bus timetable date

# it should load in the timetable data from the SQL db of timetables

# then it should generate versions that approximate the transaction style

# this should be done for each operator, for each route, for each day of week

###################
# the function will download a list of distinct operator/route combinations

# Initial SQL DB query:
# using this list it will query the database to return bus journeys 
# that are different "directions" in the tt 
# and are as close as possible 
# but do not overlap
# the stop number in the series (ascending for each next stop in the route)
# will be divided by 3 to approximate the fare stage
#
# R manipulation:
# the stage numbers of each "inbound journey" will be reversed
# 
# SQL output data creation:
# a full version will be uploaded
# a version of a random sample of 20% of rows will be uploaded
# to represent similar coverage to the original data

