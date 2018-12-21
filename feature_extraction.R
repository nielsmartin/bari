# FEATURE EXTRACTION FROM AN EVENT LOG
# Niels Martin, Hasselt University
# Implementation 1.0
# Purpose: this script provides functions to extract features from an event log,
# which will be used as predictor variables when discovering batch activation rules

# This script requires as an input:
# (1) Extended event log, which is:
# - an event log containing start and end events
# - extended with arrival events (potentially proxied arrival time)
# - extended with batch number, indicating how cases are batched

# IMPORT LIBRARIES
library(reshape)
library(dplyr)
library(lubridate)



# PART 1: SUPPORTING FUNCTIONS
# (usable for event logs containing batch number and arrival events)

# CONVERT DURATION
# Purpose: convert a particular duration in seconds or milliseconds to another order of magnitude
# Input: duration in seconds or milliseconds
# Output: duration in other order of magnitude
convert_duration <- function(raw_duration, unit_raw_duration, order_of_magnitude){
  if(!(unit_raw_duration %in% c("secs", "msecs"))){
    stop("The unit of the raw duration should be one of the following: secs, msecs")
  }
  
  if(unit_raw_duration == "secs"){
    if(order_of_magnitude == "mins"){
      converted_duration <- raw_duration / 60.0
    } else if(order_of_magnitude == "hours"){
      converted_duration <- raw_duration / 3600.0
    } else if(order_of_magnitude == "days"){
      converted_duration <- raw_duration / 86400.0
    } else if(order_of_magnitude == "weeks"){
      converted_duration <- raw_duration / 604800.0
    } else{
      stop("The time unit for the result should be one of the following: mins, hours, days, weeks")
    }
  } else{
    if(order_of_magnitude == "secs"){
      converted_duration <- raw_duration / 1000.0
    } else if(order_of_magnitude == "mins"){
      converted_duration <- raw_duration / 60000.0
    } else if(order_of_magnitude == "hours"){
      converted_duration <- raw_duration / 3600000.0
    } else if(order_of_magnitude == "days"){
      converted_duration <- raw_duration / 86400000.0
    } else if(order_of_magnitude == "weeks"){
      converted_duration <- raw_duration / 604800000.0
    } else{
      stop("The time unit for the result should be one of the following: secs, mins, hours, days, weeks")
    }
  }
  return(converted_duration)
}


# ADD HELPER COLUMNS TO ACTIVITY LOG
# Purpose: add helper columns to the activity log, which makes the calculation of particular features more efficient
# Input: activity log
# Output: activity log with helper colums
add_helper_columns_to_activity_log <- function(activity_log){
  
  # Helper column 1: starting point of case (feature function maximum_flow_time)
  activity_log <- activity_log %>%
                    group_by(case_id) %>% 
                    mutate(case_start = min(min(arrival), min(start), na.rm = TRUE))
  
  return(activity_log)
}


# PART 2: FEATURE EXTRACTION FUNCTIONS


# NUMER OF CASES IN QUEUE (volume perspective)
# Purpose: calculcate the number of cases in the queue for a particular resource at a particular activity
# Input: activity log, activity that is considered, resource that is considered, time at which the
# number of cases in the queue needs to be calculated, indication whether activity has a single queue or dedicated queues
# Output: number of cases queueing for a particular resource at a particular activity
n_cases_in_queue <- function(activity_log, act, res, timestamp, single_queue = FALSE){
  
  if(single_queue == FALSE){
    n_cases_in_queue <- as.numeric(nrow(activity_log %>% 
                                              filter(activity == act, resource == res, arrival <= timestamp, start >= timestamp)))
                                    
  } else{
    n_cases_in_queue <- as.numeric(nrow(activity_log %>% 
                                              filter(activity == act, arrival <= timestamp, start >= timestamp)))
  }
  
  return(n_cases_in_queue)
}


# TIME OF DAY (time perspective)
# Purpose: returns the time of day
# Input: timestamp
# Output: time of day
time_of_day <- function(timestamp, epoch = "1970-01-01 00:00:00 UTC"){
  
  if(is.numeric(timestamp)){
    timestamp <- as.POSIXct(timestamp, origin = epoch, tz = "GMT")
  } else if(!is.POSIXct(timestamp)){
    stop("Timestamp is not a POSIXct object. Convert timestamp to POSICXct object.")
  }
  
  if(hour(timestamp) == 0 & minute(timestamp) == 0 & second(timestamp) == 0){
    time_of_day <- "1970-01-01 00:00:00"
  } else{
    time_of_day <- as.character(timestamp)
    time_of_day <- paste("1970-01-01", strsplit(time_of_day, split = " ")[[1]][2])
  }
  
  return(time_of_day)
}


# HOUR OF DAY (time perspective)
# Purpose: returns the hour of day (time of day on a higher aggregation level)
# Input: timestamp
# Output: hour of day
hour_of_day <- function(timestamp, epoch = "1970-01-01 00:00:00 UTC"){
  
  if(is.numeric(timestamp)){
    timestamp <- as.POSIXct(timestamp, origin = epoch, tz = "GMT")
  } else if(!is.POSIXct(timestamp)){
    stop("Timestamp is not a POSIXct object. Convert timestamp to POSICXct object.")
  }
  
  hour <- hour(timestamp)
  
  return(hour)
}


# PART OF DAY (time perspective)
# Purpose: returns the part of day (time of day on a higher aggregation level)
# Input: timestamp
# Output: time of day
part_of_day <- function(timestamp, epoch = "1970-01-01 00:00:00 UTC"){
  
  if(is.numeric(timestamp)){
    timestamp <- as.POSIXct(timestamp, origin = epoch, tz = "GMT")
  } else if(!is.POSIXct(timestamp)){
    stop("Timestamp is not a POSIXct object. Convert timestamp to POSICXct object.")
  }
  
  hour <- hour(timestamp)
  
  if(hour <= 7){
    part_of_day <- "night"
  } else if(hour <= 10){
    part_of_day <- "morning"
  } else if(hour <= 12) {
    part_of_day <- "noon"
  } else if(hour <= 15){
    part_of_day <- "afternoon"
  } else if(hour <= 18){
    part_of_day <- "evening"
  } else{
    part_of_day <- "night"
  }
  
  return(part_of_day)
}


# DAY OF WEEK (time perspective)
# Purpose: return the day of the week
# Input: timestamp
# Output: day of the week
day_of_week <- function(timestamp, epoch = "1970-01-01 00:00:00 UTC"){
  
  if(is.numeric(timestamp)){
    timestamp <- as.POSIXct(timestamp, origin = epoch, tz = "GMT")
  } else if(!is.POSIXct(timestamp)){
    stop("Timestamp is not a POSIXct object. Convert timestamp to POSICXct object.")
  }
  
  day_of_week <- as.character(wday(timestamp, label = TRUE))
  
  return(day_of_week)
}


# WAITING TIME FOR THE LONGEST QUEUEING CASE (time perspective)
# Purpose: calculate the waiting time for the longest queueing case for a particular 
# resource-activity combination at a particular time 
# Input: activity log, activity that is considered, resource that is considered, time at which the
# calculation should be made, indication whether activity has a single queue or dedicated queues, time unit
# in which results should be expressed
# Output: waiting time for the longest queueing case for a particular resource-activity combination
waiting_time_longest_queueing_case <- function(activity_log, act, res, timestamp, single_queue = FALSE, timestamp_unit = NA, time_unit_result = "hours"){
  
  # Check if a valid time unit is provided
  if(!(time_unit_result %in% c("secs", "mins", "hours","days", "weeks"))){
    stop("The time unit for the result should be one of the following: secs, mins, hours, days, weeks")
  }
  
  # Check if queueing cases are present
  if(single_queue == FALSE){
    queueing_cases <- as.data.frame(activity_log %>% 
                                      filter(activity == act, resource == res, arrival <= timestamp, start >= timestamp))
  } else{
    queueing_cases <- as.data.frame(activity_log %>% 
                                      filter(activity == act, arrival <= timestamp, start >= timestamp))
  }
  
  # If queueing cases are present, calculate waiting time of longest queueing case
  if(nrow(queueing_cases) == 0){
    max_waiting_time <- NA
  } else{
    if(is.numeric(timestamp) & is.numeric(queueing_cases$arrival[1])){
      max_waiting_time <- timestamp - min(queueing_cases$arrival)
      if(time_unit_result != timestamp_unit){
        max_waiting_time <- convert_duration(max_waiting_time, timestamp_unit, time_unit_result)
      }
    } else if(is.POSIXct(timestamp) & is.POSIXct(queueing_cases$arrival[1])){
      max_waiting_time <- as.numeric(difftime(timestamp, min(queueing_cases$arrival), units = time_unit_result))
    } else{
      stop("All timestamps need to be expressed as numeric object or as POSIXct object")
    }
  }

  return(max_waiting_time)
}


# MEAN WAITING TIME FOR QUEUEING CASES (time perspective)
# Purpose: calculate the mean waiting time for the queueing cases for a particular 
# resource-activity combination at a particular time 
# Input: activity log, activity that is considered, resource that is considered, time at which the
# calculation should be made, indication whether activity has a single queue or dedicated queues, time unit
# in which results should be expressed
# Output: mean waiting time for the longest queueing cases for a particular resource-activity combination
mean_waiting_time_queueing_cases <- function(activity_log, act, res, timestamp, single_queue = FALSE, timestamp_unit = NA, time_unit_result = "hours"){
  
  # Check if a valid time unit is provided
  if(!(time_unit_result %in% c("secs", "mins", "hours","days", "weeks"))){
    stop("The time unit for the result should be one of the following: secs, mins, hours, days, weeks")
  }
  
  # Check if queueing cases are present
  if(single_queue == FALSE){
    queueing_cases <- as.data.frame(activity_log %>% 
                                      filter(activity == act, resource == res, arrival <= timestamp, start >= timestamp))
  } else{
    queueing_cases <- as.data.frame(activity_log %>% 
                                      filter(activity == act, arrival <= timestamp, start >= timestamp))
  }
  
  # If queueing cases are present, calculate waiting time of longest queueing case
  if(nrow(queueing_cases) == 0){
    mean_waiting_time <- NA
  } else{
    if(is.numeric(timestamp) & is.numeric(queueing_cases$arrival[1])){
      queueing_cases$waiting_time <- timestamp - queueing_cases$arrival
      mean_waiting_time <- mean(queueing_cases$waiting_time)
      if(time_unit_result != timestamp_unit){
        mean_waiting_time <- convert_duration(mean_waiting_time, timestamp_unit, time_unit_result)
      }
    } else if(is.POSIXct(timestamp) & is.POSIXct(queueing_cases$arrival[1])){
      queueing_cases$waiting_time <- as.numeric(difftime(timestamp, queueing_cases$arrival, units = time_unit_result))
      mean_waiting_time <- mean(queueing_cases$waiting_time)
    } else{
      stop("All timestamps need to be expressed as numeric object or as POSIXct object")
    }
  }
  
  return(mean_waiting_time)
}


# MAXIMUM FLOW TIME FOR CURRENTLY QUEUEING CASES (time perspective)
# Purpose: calculate the maximum flow time for queueing cases at a resource-activity combination at a particular time 
# Input: activity log, activity that is considered, resource that is considered, time at which the
# calculation should be made, indication whether activity has a single queue or dedicated queues, time unit
# in which results should be expressed
# Output: maximum flow time for a particular resource-activity combination
maximum_flow_time <- function(activity_log, act, res, timestamp, single_queue = FALSE, timestamp_unit = NA, time_unit_result = "hours"){
  
  # Check if a valid time unit is provided
  if(!(time_unit_result %in% c("secs", "mins", "hours","days", "weeks"))){
    stop("The time unit for the result should be one of the following: secs, mins, hours, days, weeks")
  }
  
  # Check if queueing cases are present
  if(single_queue == FALSE){
    queueing_cases <- as.data.frame(activity_log %>% 
                                      filter(activity == act, resource == res, arrival <= timestamp, start >= timestamp))
  } else{
    queueing_cases <- as.data.frame(activity_log %>% 
                                      filter(activity == act, arrival <= timestamp, start >= timestamp))
  }
  
  # If queueing cases are present, calculate waiting time of longest queueing case
  if(nrow(queueing_cases) == 0){
    max_flow_time <- NA
  } else{
    if(is.numeric(timestamp) & is.numeric(queueing_cases$case_start[1])){
      queueing_cases$flow_time <- timestamp - queueing_cases$case_start
      max_flow_time <- max(queueing_cases$flow_time)
      if(time_unit_result != timestamp_unit){
        max_flow_time <- convert_duration(max_flow_time, timestamp_unit, time_unit_result)
      }
    } else if(is.POSIXct(timestamp) & is.POSIXct(queueing_cases$case_start[1])){
      queueing_cases$flow_time <- as.numeric(difftime(timestamp, queueing_cases$case_start, units = time_unit_result))
      max_flow_time <- max(queueing_cases$flow_time)
    } else{
      stop("All timestamps need to be expressed as numeric object or as POSIXct object")
    }
  }
  
  return(max_flow_time)
}


# TIME SINCE THE LAST ARRIVAL (time perspective)
# Purpose: determine the time since the last arrival at a particular resource-activity combination
# at a particular time 
# Input: activity log, activity that is considered, resource that is considered, time at which the
# time since the last arrival should be determined, indication whether activity has a single queue or dedicated queues, time unit
# in which results should be expressed
# Output: time since the last arrival at a particular resource-activity combination
time_since_last_arrival <- function(activity_log, act, res, timestamp, single_queue = FALSE, timestamp_unit = NA, time_unit_result = "hours"){
  
  # Check if a valid time unit is provided
  if(!(time_unit_result %in% c("secs", "mins", "hours","days", "weeks"))){
    stop("The time unit for the result should be one of the following: secs, mins, hours, days, weeks")
  }
  
  # Determine the cases that have already arrived at time t
  if(single_queue == FALSE){
    arrived_cases <- as.data.frame(activity_log %>% 
                                      filter(activity == act, resource == res, arrival <= timestamp))
  } else{
    arrived_cases <- as.data.frame(activity_log %>% 
                                      filter(activity == act, arrival <= timestamp))
  }
  
  # If cases have already arrived at time t, calculate the time since the last arrival 
  if(nrow(arrived_cases) == 0){
    time_since_last_arrival <- NA
  } else{
    if(is.numeric(timestamp) & is.numeric(arrived_cases$arrival[1])){
      time_since_last_arrival <- timestamp - max(arrived_cases$arrival)
      if(time_unit_result != timestamp_unit){
        time_since_last_arrival <- convert_duration(time_since_last_arrival, timestamp_unit, time_unit_result)
      }
    } else if(is.POSIXct(timestamp) & is.POSIXct(arrived_cases$arrival[1])){
      time_since_last_arrival <- as.numeric(difftime(timestamp, max(arrived_cases$arrival), units = time_unit_result))
    } else{
      stop("All timestamps need to be expressed as numeric object or as POSIXct object")
    }
  }
  
  return(time_since_last_arrival)
}


# RESOURCE WORKLOAD (resource perspective)
# Purpose: determine the workload (i.e. the number of queueing cases) for a particular resource
# Input: activity log, resource that is considered, time at which the calculation should be made,
# indication whether activity has a single queue or dedicated queues
# Output: number of cases queueing for a particular resource at a particular activity
resource_workload <- function(activity_log, res, timestamp, single_queue = FALSE){
  
  # Determine which activities res performs
  activities <- unique((activity_log %>% filter(resource == res))$activity)

  # Determine resource workload
  workload <- (activity_log %>%
                filter(activity %in% activities, arrival <= timestamp, start >= timestamp) %>% 
                group_by(activity, resource) %>%
                summarize(n = n(), single_queue = single_queue[1]) %>%
                filter((single_queue == TRUE) | (single_queue == FALSE & resource == res)) %>%
                summarize(sum = sum(n)) %>% summarize(sum = sum(sum)))$sum[1]
  
  return(workload)
}


# CASE ATTRIBUTES LAST ARRIVING CASE (case perspective)
# Purpose: determine case attributes of the last case that arrived at a particular activity for a particular resource
# Input: activity log, activity that is considered, resource that is considered, time at which the last arriving case should be determined,
# indication whether activity has a single queue or dedicated queues
# Output: case attributes of last arrived case for a particular resource at a particular activity
case_attributes_last_arriving_case <- function(activity_log, act, res, timestamp, single_queue = FALSE){
  
  # Determine the cases that have already arrived at time t and select its case attribute columns
  if(single_queue == FALSE){
    last_arriving_case_attr<- as.data.frame(activity_log %>% 
                                        arrange(arrival) %>%
                                        filter(activity == act, resource == res, arrival <= timestamp) %>%
                                        filter(arrival == max(arrival)) %>% 
                                        select(grep("cattr#", colnames(activity_log))))
                        
  } else{
    last_arriving_case_attr<- as.data.frame(activity_log %>%
                                         arrange(arrival) %>%
                                         filter(activity == act, arrival <= timestamp) %>%
                                         filter(arrival == max(arrival)) %>%
                                         select(grep("cattr#", colnames(activity_log))))
  }
  
  # In case no cases have arrived yet, return NA for each attribute value. 
  if(nrow(last_arriving_case_attr) == 0){
    last_arriving_case_attr[1,] <-  rep(NA,ncol(last_arriving_case_attr))
  } 
  
  return(last_arriving_case_attr)
}


# AGGREGATED CASE ATTRIBUTES OF QUEUING CASES (case perspective)
# Purpose: sum case attribute values of cases queueing for a particular resource-activity combination at a particular time 
# Input: activity log, activity that is considered, resource that is considered, time at which the
# calculation should be made, indication whether activity has a single queue or dedicated queues
# Output: sum of case attribute values of cases queueing for a particular resource-activity combination at a particular time
aggregated_case_attributes_queueing_cases <- function(activity_log, act, res, timestamp, single_queue = FALSE){
  
  
  # Check if queueing cases are present and retrieve case attributes
  if(single_queue == FALSE){
    queueing_cases_attributes <- as.data.frame(activity_log %>% 
                                      filter(activity == act, resource == res, arrival <= timestamp, start >= timestamp) %>%
                                      select(grep("cattr#", colnames(activity_log))))
  } else{
    queueing_cases_attributes <- as.data.frame(activity_log %>% 
                                      filter(activity == act, arrival <= timestamp, start >= timestamp) %>%
                                      select(grep("cattr#", colnames(activity_log))))
  }
  
  # In case no cases are currently queueing, return NA for each attribute value.
  if(nrow(queueing_cases_attributes) == 0){
    queueing_cases_attributes <- NA
  } else{
    # If queueing cases are present, aggregate case attributes
    n_attributes <- ncol(queueing_cases_attributes)
    for(i in 1:n_attributes){
      
      if(is.numeric(queueing_cases_attributes[,i])){
        # Numerical case attributes
        aggr_attr_col_name <- paste("aggr_", colnames(queueing_cases_attributes)[i], sep = "")
        queueing_cases_attributes[,aggr_attr_col_name] <- sum(queueing_cases_attributes[,i])
      } else{
        # Categorical case attributes
        factor_levels <- levels(queueing_cases_attributes[,i])
        for(j in 1:length(factor_levels)){
          aggr_attr_col_name <- paste("aggr_", colnames(queueing_cases_attributes)[i], "_", factor_levels[j], sep = "")
          queueing_cases_attributes[,aggr_attr_col_name] <- length((which(queueing_cases_attributes[,i] == factor_levels[j])))
        }
      } 
    }
    
    # First row contains aggregated values
    queueing_cases_attributes <- queueing_cases_attributes[1,]
    
    
    # Remove original case attribute columns
    if(ncol(queueing_cases_attributes) > 2){
      queueing_cases_attributes <- queueing_cases_attributes[,-(1 : n_attributes)]
    } else{
      # Avoid conversion to vector when a single column is retained
      col_name <- names(queueing_cases_attributes)[2]
      queueing_cases_attributes <- as.data.frame(queueing_cases_attributes[,-(1 : n_attributes)])
      names(queueing_cases_attributes)[1] <- col_name
    }
  }
  
  return(queueing_cases_attributes)
}


# ACTIVITY EXECUTION COUNTER (case perspective)
# Purpose: add activity execution counter (instance number related to a case-activity combination) as a case attribute
# Input: activity log 
# Output: activity log with activity execution counter as an additional case attribute
add_activity_execution_counter <- function(activity_log){
  
  activity_log$"cattr#act_execution_counter" <- NA
  case_act_comb <- unique(activity_log[,c('activity', 'case_id')])
  
  for(i in 1:nrow(case_act_comb)){
    index <- activity_log$case_id == case_act_comb$case_id[i] & 
              activity_log$activity == case_act_comb$activity[i]
    activity_log$"cattr#act_execution_counter"[index] <- seq(1,as.numeric(table(index)["TRUE"]))
  }
  remove(index)
  
  return(activity_log)
}


# NUMBER OF RUNNING CASES THAT WILL ARRIVE (system state perspective)
# Purpose: calculate the number of running cases that will arrive at the activity under consideration
# Input: activity log, activity that is considered, resource that is considered, time at which the
# calculation has to be made, indication whether activity has a single queue or dedicated queues
# Output: number of running cases that will arive at the activity under consideration at time t
n_running_cases <- function(activity_log, act, res, timestamp, single_queue = FALSE){
  
  # Determine cases that will arrive at the activity under consideration
  if(single_queue == FALSE){
    arriving_cases <- (activity_log %>%
                            filter(activity == act, resource == res, arrival > timestamp))$case_id
  } else{
    arriving_cases <- (activity_log %>%
                         filter(activity == act, arrival > timestamp))$case_id
  }
  
  # Determine which arriving cases are running cases at time timestamp and count them
  n_running_cases <- (activity_log %>%
                        filter(case_id %in% arriving_cases) %>%
                        group_by(case_id) %>%
                        summarize(case_arrival_time = min(arrival)) %>%
                        filter(case_arrival_time <= timestamp) %>%
                        summarize(n = n()))$n[1]
  
  return(n_running_cases)
}



# PART 3: CALLING FUNCTIONS


# MAKE FEATURE SNAPSHOT
# Purpose: calculate all features
# Input: activity log, feature table, additional parameters
# Output: named list containing the values of all calculated features
create_feature_snapshot <- function(activity_log, feature_table, act, res, timestamp, single_queue, timestamp_unit, time_unit_result, aggregate_case_attr){
  
  next_ft_line <- nrow(feature_table) + 1
  feature_table[next_ft_line,] <- NA
  feature_table[next_ft_line,"n_in_queue"] <- n_cases_in_queue(activity_log = activity_log, act = act, res = res, timestamp = timestamp, single_queue = single_queue)
  feature_table[next_ft_line,"time_of_day"] <- time_of_day(timestamp = timestamp)
  feature_table[next_ft_line,"hour_of_day"] <- hour_of_day(timestamp = timestamp)
  feature_table[next_ft_line,"part_of_day"] <- part_of_day(timestamp = timestamp)
  feature_table[next_ft_line,"day_of_week"] <- day_of_week(timestamp = timestamp)
  feature_table[next_ft_line,"wt_longest_queueing_case"] <- waiting_time_longest_queueing_case(activity_log = activity_log, act = act, res = res, timestamp = timestamp, single_queue = single_queue, timestamp_unit = timestamp_unit, time_unit_result = time_unit_result)
  feature_table[next_ft_line,"mean_wt_queueing_cases"] <- mean_waiting_time_queueing_cases(activity_log = activity_log, act = act, res = res, timestamp = timestamp, single_queue = single_queue, timestamp_unit = timestamp_unit, time_unit_result = time_unit_result)
  feature_table[next_ft_line,"time_since_last_arrival"] <- time_since_last_arrival(activity_log = activity_log, act = act, res = res, timestamp = timestamp, single_queue = single_queue, timestamp_unit = timestamp_unit, time_unit_result = time_unit_result)
  feature_table[next_ft_line,"maximum_flow_time"] <- maximum_flow_time(activity_log = activity_log, act = act, res = res, timestamp = timestamp, single_queue = single_queue, timestamp_unit = timestamp_unit, time_unit_result = time_unit_result)
  feature_table[next_ft_line,"res_workload"] <-  resource_workload(activity_log = activity_log, res = res, timestamp = timestamp, single_queue = single_queue)
  feature_table[next_ft_line,"n_running_cases"] <- n_running_cases(activity_log = activity_log, act = act, res = res, timestamp = timestamp, single_queue = single_queue)
  
  # If the activity log contains case attributes
  if(length(grep("cattr#", colnames(activity_log)) > 0)){

    # Add case attributes of last arriving case
    case_attributes_last_arriving_case <- case_attributes_last_arriving_case(activity_log = activity_log, act = act, res = res, timestamp = timestamp, single_queue = single_queue)
    for(i in 1:ncol(case_attributes_last_arriving_case)){
      feature_table[next_ft_line, match(colnames(case_attributes_last_arriving_case)[i],colnames(feature_table))] <- case_attributes_last_arriving_case[1, colnames(case_attributes_last_arriving_case)[i]]
    }
    
    if(aggregate_case_attr == TRUE){
      # Add aggregated case attributes of queueing cases
      aggr_case_attr_queueing_cases <- aggregated_case_attributes_queueing_cases(activity_log = activity_log, act = act, res = res, timestamp = timestamp, single_queue = single_queue)
      if(!suppressWarnings(is.na(aggr_case_attr_queueing_cases))){
        for(i in 1:ncol(aggr_case_attr_queueing_cases)){
          feature_table[next_ft_line, match(colnames(aggr_case_attr_queueing_cases)[i],colnames(feature_table))] <- aggr_case_attr_queueing_cases[1, colnames(aggr_case_attr_queueing_cases)[i]]
        }
      }
    }
    
  }
  
  return(feature_table)
}



