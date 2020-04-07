#write function version here
library(RPostgreSQL)
library(data.table)
library(lubridate)

#need to rewrite this one to use the SQL version. 
#Sort out when have generated a generic version from the testing file

#now make the for loop a function

assign_dir <- function(input_data_table, same_journey_mins, same_stage_mins, max_stage, min_stage){
  
  # input data table must have the columns: 
  # tran_time POSIXlt, fare_stage int
  # 
  # same_journey_mins 
  # sets how long away a transaction in the expected order must be 
  # before it is automatically excluded as being part of the same route
  # 
  # same_stage_mins 
  # sets how long away a transaction must be from the current one 
  # before it is excluded from being considered part of the same
  # 
  # min_stage and max_stage
  # these are the minimum and maximum possible fare stage for the given bus route
  # htey are used to calibrate the start and ends of journeys
  # the highest and lowest stages can be ambiguous in terms of order 
  # as they are close and appear at the beginning and end of runs 
  # so at risk of being missassigned
  #
  
  # 
  # you need to make another version of this with varying amounts
  
  # This funciont looks at transactions for a given machine on a given route, ordered by time
  # by checking the fare stage information of each transaction 
  # against the one before as well as the 3 after it, it works out the direction of travel.
  #
  # 
  
  #make sure the data table is in the right order
  input_data_table <- input_data_table[order(tran_time)]
  
  #make the journey and stage window parameters
  same_journey_window <- duration(same_journey_mins, units = "minutes")
  same_stage_window <- duration(same_stage_mins, units = "minutes")
  
  #add the new direction and journey number columns
  input_data_table$new_dir <- "blank"
  input_data_table$new_jorno <- 0
  
  #now the for loop
  for(i in 1:nrow(input_data_table)){
    print(i)
    #statement to assign direction and journey number
    if((i==1)
       && ((((input_data_table$fare_stage[i+1]-input_data_table$fare_stage[i])>0) 
            && (input_data_table$tran_time[i+1]<(input_data_table$tran_time[i]+same_journey_window)))
           | ((input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i]) 
              && ((input_data_table$fare_stage[i+2]-input_data_table$fare_stage[i])>0) 
              && (input_data_table$tran_time[i+2]<(input_data_table$tran_time[i]+same_journey_window)))
           | ((input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
              && (input_data_table$fare_stage[i+2]==input_data_table$fare_stage[i])
              && ((input_data_table$fare_stage[i+3]-input_data_table$fare_stage[i])>0) 
              && (input_data_table$tran_time[i+3]<(input_data_table$tran_time[i]+same_journey_window))))
    ){
      #checks whether any of the next three points is increasing and within journey window
      #if so assigns out
      
      direction<- "out"
    }else if((i==1)
             && ((((input_data_table$fare_stage[i+1]-input_data_table$fare_stage[i])<0) 
                  && (input_data_table$tran_time[i+1]<(input_data_table$tran_time[i]+same_journey_window)))
                 | ((input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i]) 
                    && ((input_data_table$fare_stage[i+2]-input_data_table$fare_stage[i])<0) 
                    && (input_data_table$tran_time[i+2]<(input_data_table$tran_time[i]+same_journey_window)))
                 | ((input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
                    && (input_data_table$fare_stage[i+2]==input_data_table$fare_stage[i])
                    && ((input_data_table$fare_stage[i+3]-input_data_table$fare_stage[i])<0) 
                    && (input_data_table$tran_time[i+3]<(input_data_table$tran_time[i]+same_journey_window))))){
      direction <- "in"
      
      #like above but for decreasing
      
    }else if((i==1) 
             && (input_data_table$tran_time[i+1]>(input_data_table$tran_time[i]+same_journey_window))
             && (input_data_table$tran_time[i]>(input_data_table$tran_time[i-1]+same_journey_window))){
      #checks if the transaction if too far from any other transaction to be compared
      
      direction <- "unknown"
    }else if((input_data_table$fare_stage[i]==min_stage && input_data_table$fare_stage[i-1]!=min_stage) 
             | (input_data_table$fare_stage[i]==max_stage && input_data_table$fare_stage[i-1]!=max_stage)
    ){
      #if the transaction is at the minimum or maximum farestage
      # don't know why the second part of this is in there about cheking that the previous point is not min/max stage
      direction <- "unknown"
    }else if(input_data_table$tran_time[i]==input_data_table$tran_time[i-1]){
      #sanity check for a time error in sample set 
      #(transaction time rounding led to a lower fare stage being assinged the same time as a higher one or vice versa)
      direction <- direction
    }else if(((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])<(input_data_table$tran_time[i+1]-input_data_table$tran_time[i])) | (i==nrow(input_data_table))) 
              && ((input_data_table$fare_stage[i]-input_data_table$fare_stage[i-1])>0) 
              && (input_data_table$tran_time[i]<(input_data_table$tran_time[i-1]+same_journey_window))
              && (input_data_table$fare_stage[i]>1))
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+1]-input_data_table$tran_time[i]))) 
                && (i<nrow(input_data_table))
                && ((input_data_table$fare_stage[i+1]-input_data_table$fare_stage[i])>0) 
                && (input_data_table$tran_time[i+1]<(input_data_table$tran_time[i]+same_journey_window))) 
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+2]-input_data_table$tran_time[i]))) 
                && (i<(nrow(input_data_table)-1))
                && (input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
                && ((input_data_table$fare_stage[i+2]-input_data_table$fare_stage[i])>0) 
                && (input_data_table$tran_time[i+2]<(input_data_table$tran_time[i]+same_journey_window))) 
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+3]-input_data_table$tran_time[i])))
                && (i<(nrow(input_data_table)-2))
                && (input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
                && (input_data_table$fare_stage[i+2]==input_data_table$fare_stage[i])
                && ((input_data_table$fare_stage[i+3]-input_data_table$fare_stage[i])>0) 
                && (input_data_table$tran_time[i+3]<(input_data_table$tran_time[i]+same_journey_window)))
             | ((input_data_table$fare_stage[i]!=min_stage) 
                && (input_data_table$fare_stage[i-1]==min_stage) 
                && (input_data_table$tran_time[i]<(input_data_table$tran_time[i-1]+same_journey_window)))
    ){
      #see if the direction is out
      #compares against the point before and the 3 following points 
      #for relative positions in the fare stage
      #turning this into a data.table commange would sort out the iterability of this
      direction <- "out"
    }else if(((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])<(input_data_table$tran_time[i+1]-input_data_table$tran_time[i])) | i==nrow(input_data_table)) 
              && ((input_data_table$fare_stage[i]-input_data_table$fare_stage[i-1])<0)
              && (input_data_table$tran_time[i]<(input_data_table$tran_time[i-1]+same_journey_window))) 
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+1]-input_data_table$tran_time[i]))) 
                && (i<nrow(input_data_table)) 
                && ((input_data_table$fare_stage[i+1]-input_data_table$fare_stage[i])<0) 
                && (input_data_table$tran_time[i+1]<(input_data_table$tran_time[i]+same_journey_window)))
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+2]-input_data_table$tran_time[i])))
                && (i<(nrow(input_data_table)-1))
                && (input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
                && ((input_data_table$fare_stage[i+2]-input_data_table$fare_stage[i])<0) 
                && (input_data_table$tran_time[i+2]<(input_data_table$tran_time[i]+same_journey_window)))
             | ((((input_data_table$tran_time[i]-input_data_table$tran_time[i-1])>(input_data_table$tran_time[i+3]-input_data_table$tran_time[i]))) 
                && (i<(nrow(input_data_table)-2))
                && (input_data_table$fare_stage[i+1]==input_data_table$fare_stage[i])
                && (input_data_table$fare_stage[i+2]==input_data_table$fare_stage[i])
                && ((input_data_table$fare_stage[i+3]-input_data_table$fare_stage[i])<0) 
                && (input_data_table$tran_time[i+3]<(input_data_table$tran_time[i]+same_journey_window)))
             | ((input_data_table$fare_stage[i]!=max_stage) 
                && (input_data_table$fare_stage[i-1]==max_stage) 
                && (input_data_table$tran_time[i]<(input_data_table$tran_time[i-1]+same_journey_window)))
    ){
      #like above but for fare stages going down
      
      direction <- "in"
    }else if((input_data_table$fare_stage[i]==input_data_table$fare_stage[i-1])
             && (input_data_table$tran_time[i]>(input_data_table$tran_time[i-1]+same_stage_window))
             && (input_data_table$tran_time[i+1]>(input_data_table$tran_time[i]+same_stage_window))
    ){
      #check that the transaction is within reasonable distance of the previous one
      #if not assign unkown
      
      direction <- "unknown"
    }else if(((input_data_table$fare_stage[i]==input_data_table$fare_stage[i-1]) 
              && (input_data_table$tran_time[i]<(input_data_table$tran_time[i-1]+same_stage_window)))
    ){
      #if the transaction is within the stage window of the previous one and 
      #none of the other transactions relate to it then assign same as previous
      
      direction <- direction
    }
    
    #assign direction
    input_data_table$new_dir[i] <- direction
    
    #if direction is different from previous one add 1 to journey counter
    if(i==1){
      journey_counter <- 1
    }else if(input_data_table$new_dir[i]!=input_data_table$new_dir[i-1]){
      journey_counter <- journey_counter+1
    }
    #then statement to add these to the new columns
    input_data_table$new_jorno[i] <- journey_counter
    
    
  }
  
  #return the result
  return(input_data_table)
}

