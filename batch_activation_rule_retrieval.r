# EVENT LOG BASED BATCH ACTIVATION RULE RETRIEVAL
# Niels Martin, Hasselt University
# Implementation 1.0
# Purpose: this script provides functions to prepare an event log for analysis
# and retrieve batch activation rules from them

# This script requires as an input:
# (1) Event log, containing start and complete events
# (2) Functions from feature_extraction.r
source("feature_extraction.R")

# IMPORT LIBRARIES
library(reshape)
library(dplyr)
library(lubridate)


# PART 1: SUPPORTING FUNCTIONS TO PREPARE EVENT LOG FOR FEATURE EXTRACTION
# (usable for event logs without batch number and with/without arrival events)


# RENAME EVENT LOG
rename_event_log <- function(event_log, timestamp, case_id, activity, resource, event_type, activity_instance_id = NULL, case_attributes = NULL,
                             batch_number = NULL, arrival_events_recorded = FALSE, event_type_arrival = "arrival", event_type_start = "start", 
                             event_type_complete = "complete") {
  colnames(event_log)[match(timestamp, colnames(event_log))] <- "timestamp"
  colnames(event_log)[match(case_id, colnames(event_log))] <- "case_id"
  colnames(event_log)[match(activity, colnames(event_log))] <- "activity"
  colnames(event_log)[match(resource, colnames(event_log))] <- "resource"
  colnames(event_log)[match(event_type, colnames(event_log))] <- "event_type"
  
  if(!is.null(case_attributes)){
    for(i in 1:length(case_attributes)){
      colnames(event_log)[match(case_attributes[i], colnames(event_log))] <- paste("cattr#", case_attributes[i], sep = "")
    }
  }
  
  if(!is.null(batch_number)){
    colnames(event_log)[match(batch_number, colnames(event_log))] <- "batch_number"
  }
  
  if(!is.null(activity_instance_id)){
    colnames(event_log)[match(activity_instance_id, colnames(event_log))] <- "activity_instance_id"
  }
  
  if(arrival_events_recorded == TRUE & event_type_arrival != "arrival"){
    index <- event_log$event_type == event_type_arrival
    event_log$event_type[index] <- "arrival"
    remove(index)
  }
  if(event_type_start != "start"){
    index <- event_log$event_type == event_type_start
    event_log$event_type[index] <- "start"
    remove(index)
  }
  if(event_type_complete != "complete"){
    index <- event_log$event_type == event_type_complete
    event_log$event_type[index] <- "complete"
    remove(index)
  }
  
  return(event_log)
}


# ADD ARRIVAL EVENTS TO THE EVENT LOG
add_arrival_events <- function(renamed_event_log, previous_in_trace = TRUE){
  
  if(previous_in_trace == FALSE){
    stop("Sorry, this is not supported yet.")
  }
  
  # If timestamps are given in POSIXct format, convert it to character to avoid
  # copying issues. It will be reconverted to POSIXct for, e.g., feature extraction.
  if(is.POSIXct(timestamp)){
    renamed_event_log$timestamp <- as.character(renamed_event_log$timestamp)
  }
  
  # Case arrival is equated to the last complete timestamp recorded in the case's trace
  if(previous_in_trace == TRUE){
    
    # Order event log
    renamed_event_log <- renamed_event_log %>% arrange(timestamp, activity, desc(event_type))
    
    # Add row number
    renamed_event_log$row_id <- seq(1, nrow(renamed_event_log))
    
    # Select start events
    start_events <- renamed_event_log %>% filter(event_type == "start")
    
    # For each start event, add the accompanying arrival event
    for(i in 1:nrow(start_events)){
      
      # Check whether an arrival proxy can be determined
      
      proxy_events <- as.data.frame(renamed_event_log %>%
                                      filter(case_id == start_events$case_id[i], event_type == "complete",
                                             timestamp <= start_events$timestamp[i], row_id < start_events$row_id[i]))
      if(nrow(proxy_events) > 0){
        arrival_proxy <- (proxy_events %>% 
                            summarize(arrival_proxy = max(timestamp)))$arrival_proxy[1]
        
        next_log_line <- nrow(renamed_event_log) + 1
        renamed_event_log[next_log_line,] <- start_events[i,]
        renamed_event_log$event_type[next_log_line] <- "arrival"
        renamed_event_log$timestamp[next_log_line] <- arrival_proxy
      }
    }
    
    # Remove row_id
    renamed_event_log$row_id <- NULL
    
    # Order event log
    renamed_event_log <- renamed_event_log %>% arrange(timestamp)
  }
  
  return(renamed_event_log)
}


# ADD SINGLE QUEUE MARKER TO THE EVENT LOG
add_single_queue_marker <- function(renamed_event_log){
  
  renamed_event_log$single_queue <- FALSE
  
  return(renamed_event_log)
}


# CHANGE SINGLE QUEUE MARKER TO LOG
mark_single_queue <- function(renamed_event_log, activity){
  
  index <- renamed_event_log$activity == activity
  renamed_event_log$single_queue[index] <- TRUE
  remove(index)
  
  return(renamed_event_log)
}


# CONVERT EVENT LOG TIMESTAMP FORMAT TO POSIXCT OBJECT
convert_timestamp_format <- function(renamed_event_log, timestamp_format = "yyyy-mm-dd hh:mm:ss"){
  
  if(timestamp_format == "yyyy-mm-dd hh:mm:ss"){
    renamed_event_log$timestamp <- ymd_hms(renamed_event_log$timestamp)
  } else if(timestamp_format == "dd-mm-yyyy hh:mm:ss"){
    renamed_event_log$timestamp <- dmy_hms(renamed_event_log$timestamp)
  } else if(timestamp_format == "dd-mm-yyyy hh:mm"){
    renamed_event_log$timestamp <- dmy_hm(renamed_event_log$timestamp)
  } else{
    stop("Timestamp format not supported. Convert timestamp to POSIXct object manually.")
  }
  
  return(renamed_event_log)
}


# CONVERT ACTIVITY LOG TIMESTAMP FORMAT TO POSIXCT OBJECT
convert_activity_log_timestamp_format <- function(activity_log, timestamp_type, timestamp_format = "yyyy-mm-dd hh:mm:ss"){
  
  if(timestamp_format == "yyyy-mm-dd hh:mm:ss"){
    activity_log[, timestamp_type] <- ymd_hms(activity_log[, timestamp_type])
  } else if(timestamp_format == "dd-mm-yyyy hh:mm:ss"){
    activity_log[, timestamp_type] <- dmy_hms(activity_log[, timestamp_type])
  } else if(timestamp_format == "dd-mm-yyyy hh:mm"){
    activity_log[, timestamp_type] <- dmy_hm(activity_log[, timestamp_type])
  } else{
    stop("Timestamp format not supported. Convert timestamp to POSIXct object manually.")
  }
  
  return(activity_log)
}


# ADD ACTIVITY INSTANCE ID TO EVENT LOG
add_activity_instance_id <- function(renamed_event_log, event_types = c("arrival", "start", "complete"), remove_incomplete_instances = FALSE){
  
  if("activity_instance_id" %in% colnames(renamed_event_log)){
    
    message("Activity instance identifier is already present in the event log")
    
  } else{
    
    n_event_types <- length(event_types)
    instance_id_value <- 0
    
    # order event log
    renamed_event_log <- renamed_event_log %>% arrange(case_id,activity,timestamp)
    
    renamed_event_log$activity_instance_id <- NA
    case_act_res_comb <- unique(renamed_event_log[,c('activity', 'resource', 'case_id')])
    
    for(i in 1:nrow(case_act_res_comb)){
      index <- renamed_event_log$case_id == case_act_res_comb$case_id[i] & renamed_event_log$activity == case_act_res_comb$activity[i] & renamed_event_log$resource == case_act_res_comb$resource[i]
      
      if(as.numeric(table(index)["TRUE"]) < n_event_types){
        # When not all events are recorded for a particular instance, action depends upon value of remove_incomplete_instances
        if(remove_incomplete_instances == TRUE){
          warning(paste("Instance", case_act_res_comb$case_id[i], " - activity", case_act_res_comb$activity[i], " - resource", case_act_res_comb$resource[i], "removed because not all events were recorded."))
          renamed_event_log <- renamed_event_log[-which(index == TRUE),]
        } else{
          stop(paste("Event log does not contain all events for case", case_act_res_comb$case_id[i], " - activity", case_act_res_comb$activity[i], " - resource", case_act_res_comb$resource[i]))
        }
        
      } else if(as.numeric(table(index)["TRUE"]) == n_event_types){
        
        instance_id_value <- instance_id_value + 1
        renamed_event_log$activity_instance_id[index] <- instance_id_value
        
      } else{
        instance_id_value <- instance_id_value + 1
        
        n_of_instances <- as.numeric(table(index)["TRUE"]) / n_event_types
        
        if(n_of_instances %% 1 != 0){
          stop(paste("Event log does not contain all events for case", case_act_res_comb$case_id[i], " - activity", case_act_res_comb$activity[i], " - resource", case_act_res_comb$resource[i]))
        }
        
        instance_id_range <- seq(instance_id_value, instance_id_value + n_of_instances - 1)
        
        for(j in 1:n_event_types){
          index2 <- renamed_event_log$case_id == case_act_res_comb$case_id[i] & 
            renamed_event_log$activity == case_act_res_comb$activity[i] &
            renamed_event_log$resource == case_act_res_comb$resource[i] &
            renamed_event_log$event_type == event_types[j]
          
          renamed_event_log$activity_instance_id[index2] <- instance_id_range
        }
        instance_id_value <- max(instance_id_range) 
      }
    }
    
    # Sort renamed event log
    renamed_event_log <- renamed_event_log %>%
      arrange(timestamp, case_id)
    
  }
  
  return(renamed_event_log)
}


# CONVERT EVENT LOG TO ACTIVITY LOG
convert_to_activity_log <- function(renamed_event_log){
  
  # Create activity log, where the statement structure depends on the presence of (i) activity instance id, (ii) case attributes and (iii) the batch number
    
  if("activity_instance_id" %in% names(renamed_event_log) & "batch_number" %in% names(renamed_event_log) & length(names(renamed_event_log)[grep("cattr", names(renamed_event_log))]) > 0){
    # Event log containing: activity instance id, case attributes, batch number
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource", "single_queue", colnames(renamed_event_log)[grep("cattr#", colnames(renamed_event_log))],
                                      "activity_instance_id", "batch_number"), 
                            direction = "wide")
  } else if("activity_instance_id" %in% names(renamed_event_log) & length(names(renamed_event_log)[grep("cattr", names(renamed_event_log))]) > 0){
    # Event log containing: activity instance id, case attributes
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource", "single_queue", colnames(renamed_event_log)[grep("cattr#", colnames(renamed_event_log))],
                                      "activity_instance_id"), 
                            direction = "wide")
  } else if("activity_instance_id" %in% names(renamed_event_log) & "batch_number" %in% names(renamed_event_log)){
    # Event log containing: activity instance id, batch number
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource", "single_queue","activity_instance_id", "batch_number"), 
                            direction = "wide")
  } else if("batch_number" %in% names(renamed_event_log) & length(names(renamed_event_log)[grep("cattr", names(renamed_event_log))]) > 0){
    # Event log containing: case attributes, batch number
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource", "single_queue", colnames(renamed_event_log)[grep("cattr#", colnames(renamed_event_log))],
                                      "batch_number"), 
                            direction = "wide")
  } else if("activity_instance_id" %in% names(renamed_event_log)){
    # Event log containing: activity instance id
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource", "single_queue","activity_instance_id"), 
                            direction = "wide")
  } else if(length(names(renamed_event_log)[grep("cattr", names(renamed_event_log))]) > 0){
    # Event log containing: case attributes
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource", "single_queue", colnames(renamed_event_log)[grep("cattr#", colnames(renamed_event_log))]), 
                            direction = "wide")
  } else if("batch_number" %in% names(renamed_event_log)){
    # Event log containing: batch number
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource", "single_queue","batch_number"), 
                            direction = "wide")
  } else{
    # Event log not containing activity instance id, batch number or case attributes
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource", "single_queue"), 
                            direction = "wide")
  }
    
  colnames(activity_log)[match("timestamp.arrival", colnames(activity_log))] <- "arrival"
  colnames(activity_log)[match("timestamp.start", colnames(activity_log))] <- "start"
  colnames(activity_log)[match("timestamp.complete", colnames(activity_log))] <- "complete"
  
  
  # Filter activity instances for which either start or complete is missing
  activity_log <- as.data.frame(activity_log %>% filter(!is.na(start), !is.na(complete)))
  
  return(activity_log)
}


# ADD BATCH NUMBER TO THE ACTIVITY LOG
# Note: seq_tolerated_gap is specified in seconds
add_batch_number_to_activity_log <- function(activity_log, seq_tolerated_gap = 0.0,  timestamp_format = "yyyy-mm-dd hh:mm:ss"){
  
  # Check if character timestamps are in POSIXct format
  if(is.character(activity_log$arrival)){
    activity_log <- convert_activity_log_timestamp_format(activity_log, "arrival", timestamp_format = timestamp_format)
  }
  if(is.character(activity_log$start)){
    activity_log <- convert_activity_log_timestamp_format(activity_log, "start", timestamp_format = timestamp_format)
  }
  if(is.character(activity_log$complete)){
    activity_log <- convert_activity_log_timestamp_format(activity_log, "complete", timestamp_format = timestamp_format)
  }
  
  # Sort activity log based on resource-activity combinations
  activity_log <- activity_log %>% arrange(activity, resource, start, complete)
  
  # Add numeric timestamps for calculation purposes
  activity_log$arrival_num <- as.numeric(activity_log$arrival)
  activity_log$start_num <- as.numeric(activity_log$start)
  activity_log$complete_num <- as.numeric(activity_log$complete)
  
  # When arrival_num is not available, set arrival_num to zero
  index <- is.na(activity_log$arrival)
  activity_log$arrival_num[index] <- 0
  remove(index)
  
  # Add batch number column in activity log
  activity_log$batch_number <- NA
  
  # Initialize values
  batch_counter <- 1
  activity_log$batch_number[1] <- batch_counter
  start_first_batched_case <- activity_log$start_num[1]
  
  # Go through activity log and add batch numbers
  for(i in 2:nrow(activity_log)){
    
    # Check batching conditions:
    # (i) activity of two subsequent activity instances are the same
    # (ii) resource of two subsequent activity instances are the same
    # (iii) one of the following applies:
    # - there is some overlap (complete or partial) between both instances in time
    # - the start timestamp (almost) equals the complete timestamp of the preeding instance and both
    # instances were present before processing of the first starts and the resource was not active
    # during the time gap that might be allowed
    
    if(activity_log$activity[i] == activity_log$activity[i-1] &
       activity_log$resource[i] == activity_log$resource[i-1] &
       (
          # Simultaneous or concurrent batch processing
         (activity_log$start_num[i] >= activity_log$start_num[i-1] & activity_log$start_num[i] < activity_log$complete_num[i-1]) |
          # Sequential batch processing
         (activity_log$start_num[i] <= activity_log$complete_num[i-1] + seq_tolerated_gap &
          activity_log$arrival_num[i] <= start_first_batched_case &
          resource_active(activity_log, activity_log$resource[i], activity_log$complete_num[i-1], activity_log$start_num[i], act = activity_log$activity[i]) == FALSE) 
       )
       ){
      
      activity_log$batch_number[i] <- batch_counter
      
    } else{
      # Increase batch counter and add this new value to the current instance
      batch_counter <- batch_counter + 1
      activity_log$batch_number[i] <- batch_counter
      
      # Save the start time of the first case in a batch
      start_first_batched_case <- activity_log$start_num[i]
    }
  }
  
  # Remove numeric timestamp columns
  activity_log$arrival_num <- NULL
  activity_log$start_num <- NULL
  activity_log$complete_num <- NULL
  
  return(activity_log)
}


# CHECK IF RESOURCE WAS ACTIVE DURING A PARTICULAR TIME FRAME
# Resource was active when: (i) a start event is recorded in this time frame not including boundaries,
# (ii) a complete event is recorded in this time frame not including boundaries or (iii) an instance
# is recorded for which start and complete exactly match the time frame boundaries
resource_active <- function(activity_log, res, start_tf, end_tf, act = NA){
  analysis_set <- activity_log %>%
    filter(resource == res, 
           (start > start_tf & start < end_tf) | (complete >  start_tf & complete < end_tf) |
           (start == start_tf & complete == end_tf))
  
  # If required, exclude executions of the same activity that is currently being analysed (relevant when an activity execution has a duration of zero)
  if(!is.na(act)){
    analysis_set <- analysis_set %>% filter(activity != act)
  }
  
  # If resource activity is recorded, return TRUE. Otherwise, return FALSE.
  if(nrow(analysis_set) > 0){
    return(TRUE)
  } else{
    return(FALSE)
  }
}


# PART 2: FEATURE EXTRACTION 

# CREATE FEATURE TABLE USING ACTIVITY LOG WITH BATCH NUMBERS
create_feature_table <- function(activity_log, timestamp_unit = NA, time_unit_result = "hours", include_singleton_batches = FALSE, aggregate_case_attr = FALSE, timestamp_format = "yyyy-mm-dd hh:mm:ss", origin_ts = "1990-01-01 00:00:00", neg_time_ex_unit = "hour", neg_time_ex_sample = NA, add_negative_examples = TRUE){
  
  # Make sure that batch numbers are included in the activity log
  if(!("batch_number" %in% names(activity_log))){
    stop("Batch numbers should be included to conduct this analysis.")
  }
  
  # If no single_queue values are included in the activity log, these are added
  # and initialized to FALSE
  if(!("single_queue" %in% names(activity_log))){
    activity_log$single_queue <- FALSE
    warning("Single queue values were added to the activity log and were initialized to FALSE.")
  }
  
  # Check if timestamps are in POSIXct format
  if(is.character(activity_log$arrival)){
    activity_log <- convert_activity_log_timestamp_format(activity_log, "arrival", timestamp_format = timestamp_format)
  }
  if(is.character(activity_log$start)){
    activity_log <- convert_activity_log_timestamp_format(activity_log, "start", timestamp_format = timestamp_format)
  }
  if(is.character(activity_log$complete)){
    activity_log <- convert_activity_log_timestamp_format(activity_log, "complete", timestamp_format = timestamp_format)
  }
  
  # Add 'case_start helper' column for the calculation of the maximum flow time feature
  activity_log <- add_helper_columns_to_activity_log(activity_log)
  
  # Convert activity log to data frame (technical operation)
  activity_log <- as.data.frame(activity_log)
  
  # Initialize feature table
  feature_table <- data.frame(resource = NA, activity = NA, feature_timestamp = NA,
                              n_in_queue = NA, time_of_day = NA, hour_of_day = NA,
                              part_of_day = NA, day_of_week = NA, wt_longest_queueing_case = NA,
                              mean_wt_queueing_cases = NA, time_since_last_arrival = NA,
                              maximum_flow_time = NA, res_workload = NA, n_running_cases = NA)
  
  # add case attribute columns when case attributes are present
  if(length(grep("cattr#", colnames(activity_log)) > 0)){
    attribute_columns <- colnames(activity_log)[grep("cattr#", colnames(activity_log))]
    for(i in 1:length(attribute_columns)){
      feature_table[,attribute_columns[i]] <- NA
    }
  }
  
  if(aggregate_case_attr == TRUE){
    # add aggregated case attribute columns when case attributes are present
    if(length(grep("cattr#", colnames(activity_log)) > 0)){
      attribute_columns <- colnames(activity_log)[grep("cattr#", colnames(activity_log))]
      for(i in 1:length(attribute_columns)){
        
        if(is.numeric(activity_log[,attribute_columns[i]])){
          # numerical case attributes
          aggr_attr_column <- paste("aggr_", attribute_columns[i], sep = "")
          feature_table[,aggr_attr_column] <- NA
        } else if(is.factor(activity_log[,attribute_columns[i]])){
          # categorical case attributes
          factor_levels <- levels(activity_log[,attribute_columns[i]])
          for(j in 1:length(factor_levels)){
            aggr_attr_column <- paste("aggr_", attribute_columns[i], "_", factor_levels[j], sep = "")
            feature_table[,aggr_attr_column] <- NA
          }
        } else{
          stop("Error. Make sure that case attributes are of type numeric or factor when aggregation is enabled.")
        }
        
        aggr_attr_column <- paste("aggr_", attribute_columns[i], sep = "")
        feature_table[,aggr_attr_column] <- NA
        
      }
    }
  }
  
  # create outcome column
  feature_table$outcome <- NA
  
  # create column indicating to which set a negative example belongs
  feature_table$neg_ex_set <- NA
  
  # Add activity log row identifier to the activity log
  activity_log$al_rowid <- seq(1, nrow(activity_log))
  
  # Depending on include_singleton_batches setting, filter the activity log
  if(include_singleton_batches == FALSE){
    batches_larger_than_size_one <- (activity_log %>% group_by(batch_number) %>% summarize(batch_size = n()) %>% filter(batch_size > 1))$batch_number
    activity_log <- activity_log %>% filter(batch_number %in% batches_larger_than_size_one)
  }
  
  # Add snapshots at times when a batch is activated
  
  activation_moments <- activity_log %>% 
    group_by(batch_number) %>% 
    filter(start == min(start), row_number() == 1)
  
  print(paste("Adding", nrow(activation_moments) ,"feature snapshots for activation moments..."))
  pb <- txtProgressBar(min = 0, max = nrow(activation_moments), style = 3)
  if(nrow(activation_moments) > 0){
    for(i in 1:nrow(activation_moments)){
      # Create feature snapshot and add it to the feature table
      feature_table <- create_feature_snapshot(activity_log = activity_log, feature_table = feature_table,
                                               act = activation_moments$activity[i], res = activation_moments$resource[i], 
                                               timestamp = activation_moments$start[i], single_queue = activation_moments$single_queue[i],
                                               timestamp_unit = timestamp_unit, time_unit_result = time_unit_result, aggregate_case_attr = aggregate_case_attr)
      feature_table$feature_timestamp[i+1] <- activation_moments$start[i]
      feature_table$resource[i+1] <- activation_moments$resource[i]
      feature_table$activity[i+1] <- activation_moments$activity[i]
      feature_table$outcome[i+1] <- "activate_batch"
      
      setTxtProgressBar(pb, i)
    }
    close(pb)
  }
  
  # When requested, add snapshots at times when a batch is not activated
  
  if(add_negative_examples == TRUE){
    
    # Add snapshots at times when a batch is not activated:
    # (i) set 1: when a case arrives when the resource is idle
    # (ii) set 2: when a batch ends and new batch does not start immediately
    # (iii) set 3: time-related negative examples
    #moments_of_non_activation <- activity_log %>% group_by(batch_number) %>% filter(arrival < min(start), start != min(start))
    
    # SET 1:
    print("Determining negative example snapshot times (set 1)...")
    snapshot_al_rowid <- c()
    # check if resource is active at the moment of arrival of a new case
    for(i in 1:nrow(activity_log)){
      res_act_at_arrival_time <- activity_log %>% filter(resource == activity_log$resource[i], start <= activity_log$arrival[i], complete >= activity_log$arrival[i])
      if(nrow(res_act_at_arrival_time) == 0){
        # if a resource is not active, then this arrival is eligible for a feature snapshot
        snapshot_al_rowid <- c(snapshot_al_rowid, activity_log$al_rowid[i])
      }
    }
    
    moments_of_non_activation_set1 <- as.data.frame(activity_log %>% filter(al_rowid %in% snapshot_al_rowid, !(al_rowid %in% activation_moments$al_rowid)))
    moments_of_non_activation_set1$snapshot_time <- moments_of_non_activation_set1$arrival
    
    # SET 2:
    print("Determining negative example snapshot times (set 2)...")
    activity_log$next_start <- c(activity_log$start[-1], NA)
    activity_log$next_resource <- c(activity_log$resource[-1], NA)
    moments_of_non_activation_set2 <- as.data.frame(activity_log %>% group_by(batch_number) %>% filter(complete == max(complete), !(al_rowid %in% activation_moments$al_rowid),
                                                                                                       !(al_rowid %in% moments_of_non_activation_set1$row_id),
                                                                                                       !(complete == next_start & resource == next_resource)))
    moments_of_non_activation_set2$snapshot_time <- moments_of_non_activation_set2$complete
    
    # SET 3:
    print("Determining negative example snapshot times (set 3)...")
    # convert origin_ts to POSIXct
    origin_ts <- as.POSIXct(origin_ts, tz="UTC")
    # start from activation moments
    set3_creation <- activation_moments
    set3_n_activ <- nrow(set3_creation)
    # if timestamps are numeric, convert timestamp to POSIXct format
    if(is.numeric(set3_creation$start)){
      set3_creation$start_num <- set3_creation$start
      set3_creation$start <- as.POSIXct(format(as.POSIXct(set3_creation$start, origin = origin_ts, tz = "UTC")), tz = "UTC")
    }
    if(is.numeric(activity_log$complete)){
      activity_log$arrival_num <- activity_log$arrival
      activity_log$start_num <- activity_log$start
      activity_log$complete_num <- activity_log$complete
      activity_log$arrival <- as.POSIXct(format(as.POSIXct(activity_log$arrival, origin = origin_ts, tz = "UTC")), tz = "UTC")
      activity_log$start <- as.POSIXct(format(as.POSIXct(activity_log$start, origin = origin_ts, tz = "UTC")), tz = "UTC")
      activity_log$complete <- as.POSIXct(format(as.POSIXct(activity_log$complete, origin = origin_ts, tz = "UTC")), tz = "UTC")
    }
    # add columns that will be helpful to determine resource idleness
    activity_log$next_start <- c(activity_log$start[2:length(activity_log$start)], NA)
    activity_log$next_res <- c(activity_log$resource[2:length(activity_log$resource)], NA)
    # save activation times in vector
    activ_times <- set3_creation$start
    # determine time boundaries of activity log
    #log_start <- min(activity_log$arrival) # for the moment, only "forward looking" will be supported
    log_end <- max(activity_log$complete)
    # for each activation moment...
    for(i in 1:set3_n_activ){
      
      neg_ex_snapshot_time <- as.POSIXct(NA)
      
      # determine timestamps that can be considered in the analysis
      if(neg_time_ex_unit %in% c("moment","hour","part_of_day")){
        times <- seq(set3_creation$start[i], log_end, 86400) # 1 hour = 86400 seconds
      } else if(neg_time_ex_unit == "day"){
        times <- seq(set3_creation$start[i], log_end, 604800) # 1 week = 604800 seconds
      } else{
        stop("Unvalid unit for negative time examples. Unit should be moment, hour, part_of_day or day.")
      }
      
      # # sample values from 'times' that will be investigated (sampling moved to moment after which activations are removed)
      # if(!is.na(neg_time_ex_sample)){
      #   times <- sort(sample(times, ceiling(neg_time_ex_sample * length(times))))
      # }
      
      # determine times at which negative examples need to be generated
      if(neg_time_ex_unit == "moment"){
        # remove times at which batch activation is recorded
        times <- setdiff(times,activ_times)
        # sample values from 'times' that will be investigated
        if(!is.na(neg_time_ex_sample)){
          times <- sort(sample(times, ceiling(neg_time_ex_sample * length(times))))
        }
        # determine whether resource is idle at 'times' and, when this is the case, if queueing cases are present
        if(length(times) > 0){
          for(a in 1:length(times)){
            res_idle <- resource_idle_moment(set3_creation$resource[i], activity_log, times[a])
            if(res_idle == TRUE){
              n_in_queue <- n_cases_in_queue(activity_log, set3_creation$activity[i], set3_creation$resource[i], times[a], set_creation$single_queue[i])
              # if cases are queueing for the resource at a moment at which the resource is idle, a negative example should be generated
              if(n_in_queue > 0){
                neg_ex_snapshot_time <- c(neg_ex_snapshot_time, times[a])
              }
            }
          }
        }
      } else if(neg_time_ex_unit == "hour"){
        # remove times at which batch activation is recorded
        temp_act_times <- paste(as.Date(set3_creation$start), hour(set3_creation$start))
        temp_times <- paste(as.Date(times),hour(times))
        temp_comparison <- temp_times %in% temp_act_times
        remove <- which(temp_comparison == TRUE)
        times <- times[-remove]
        # sample values from 'times' that will be investigated
        if(!is.na(neg_time_ex_sample)){
          times <- sort(sample(times, ceiling(neg_time_ex_sample * length(times))))
        }
        # determine whether resource was idle at 'times' and, when this is the case, if queueing cases are present
        if(length(times) > 0) {
          for(a in 1:length(times)){
            period_start <- paste(as.Date(times[a]), " ", hour(times[a]),":00:00", sep = "")
            period_end <- paste(as.Date(times[a]), " ", hour(times[a]),":59:59", sep = "")
            res_idle_tf <- resource_idle_timeframe(set3_creation$resource[i], activity_log, period_start, period_end)
            if(nrow(res_idle_tf) > 0){
              counter <- 1
              while(counter <= nrow(res_idle_tf)){
                # check if queueing cases are present at the start of the idle period...
                n_in_queue <- n_cases_in_queue(activity_log, set3_creation$activity[i], set3_creation$resource[i], res_idle_tf$start_idle_period[counter], set3_creation$single_queue[i])
                if(n_in_queue > 0){
                  neg_ex_snapshot_time <- c(neg_ex_snapshot_time, times[a])
                  counter <- nrow(res_idle_tf) + 1 #stop checking
                } else{
                  # ... Otherwise, check if a case arrives during the idle period....
                  if(set3_creation$single_queue[i] == FALSE){
                    arriving_cases <- as.data.frame(activity_log %>% filter(activity == set3_creation$activity[i], resource == set3_creation$resource[i], arrival %in% res_idle_tf$idle_interval[counter]))
                  } else{
                    arriving_cases <- as.data.frame(activity_log %>% filter(activity == set3_creation$activity[i], arrival %in% res_idle_tf$idle_interval[counter]))
                  }
                  if(nrow(arriving_cases) > 0) {
                    #... If such cases are present, this means that a negative example should be generated. 
                    neg_ex_snapshot_time <- c(neg_ex_snapshot_time, times[a])
                    counter <- nrow(res_idle_tf) + 1 #stop checking 
                  } else{
                    counter <- counter + 1 
                  }
                }
              }
              
            }
          }
        }
      } else if(neg_time_ex_unit == "part_of_day" | neg_time_ex_unit == "day"){
        stop("This negative time example unit is not yet supported by the code.")
      }
      
      # add snapshot times to set3_creation
      neg_ex_snapshot_time <- neg_ex_snapshot_time[2:length(neg_ex_snapshot_time)]
      if(length(neg_ex_snapshot_time) > 0) {
        neg_ex <- data.frame(snapshot_time = neg_ex_snapshot_time)
        neg_ex$activity <- set3_creation$activity[i]
        neg_ex$resource <- set3_creation$resource[i]
        neg_ex$single_queue <- set3_creation$single_queue[i]
        
        set3_creation <- bind_rows(set3_creation, neg_ex)
      }
      
      
    }
    
    # remove activation times from set3_creation
    set3_creation <- set3_creation %>% filter(!is.na(snapshot_time))
    
    
    # merge sets:
    #moments_of_non_activation <- rbind(moments_of_non_activation_set1, moments_of_non_activation_set2)
    
    # add non-activation snapshots for set 1
    # developer's note: in case set 2 approach changes to the situation before 12/07, just uncomment merge sets and use this instead of moment_of_non_activation_set1
    print(paste("Adding feature snapshots for", nrow(moments_of_non_activation_set1), "non-activation timestamps (set 1)..."))
    if(nrow(moments_of_non_activation_set1) > 0){
      pb <- txtProgressBar(min = 0, max = nrow(moments_of_non_activation_set1), style = 3)
      for(i in 1:nrow(moments_of_non_activation_set1)){
        # Create feature snapshot and add it to the feature table
        feature_table <- create_feature_snapshot(activity_log = activity_log, feature_table = feature_table,
                                                 act = moments_of_non_activation_set1$activity[i], res = moments_of_non_activation_set1$resource[i], 
                                                 timestamp = moments_of_non_activation_set1$snapshot_time[i], single_queue = moments_of_non_activation_set1$single_queue[i],
                                                 timestamp_unit = timestamp_unit, time_unit_result = time_unit_result, aggregate_case_attr = aggregate_case_attr)
        feature_table$feature_timestamp[nrow(feature_table)] <- moments_of_non_activation_set1$snapshot_time[i]
        feature_table$resource[nrow(feature_table)] <- moments_of_non_activation_set1$resource[i]
        feature_table$activity[nrow(feature_table)] <- moments_of_non_activation_set1$activity[i]
        feature_table$outcome[nrow(feature_table)] <- "do_not_activate_batch"
        feature_table$neg_ex_set[nrow(feature_table)] <- 1
        
        setTxtProgressBar(pb, i)
      }
      close(pb)
    }
    
    # add non-activation snapshots for set 2
    print(paste("Adding feature snapshots for", nrow(moments_of_non_activation_set2), "non-activation timestamps (set 2)..."))
    if(nrow(moments_of_non_activation_set2) > 0){
      pb <- txtProgressBar(min = 0, max = nrow(moments_of_non_activation_set2), style = 3)
      for(i in 1:nrow(moments_of_non_activation_set2)){
        
        # Determine from which activities instances are in the queue at the moment of non activation (also record whether or not it has a single queue)
        distinct_act_in_queue <- activity_log %>% filter(resource == moments_of_non_activation_set2$resource[i], 
                                                         arrival <= moments_of_non_activation_set2$snapshot_time[i],
                                                         start > moments_of_non_activation_set2$snapshot_time[i]) %>%
          group_by(activity) %>% 
          summarize(single_queue = single_queue[1])
        
        # If the queue is not empty...
        if(nrow(distinct_act_in_queue) > 0){
          
          # For each activity that is represented in the queue ...
          for(j in 1:nrow(distinct_act_in_queue)){
            # ... create feature snapshot and add it to the feature table
            feature_table <- create_feature_snapshot(activity_log = activity_log, feature_table = feature_table,
                                                     act = distinct_act_in_queue$activity[j], res = moments_of_non_activation_set2$resource[i], 
                                                     timestamp = moments_of_non_activation_set2$snapshot_time[i], single_queue = distinct_act_in_queue$single_queue[j],
                                                     timestamp_unit = timestamp_unit, time_unit_result = time_unit_result, aggregate_case_attr = aggregate_case_attr)
            feature_table$feature_timestamp[nrow(feature_table)] <- moments_of_non_activation_set2$snapshot_time[i]
            feature_table$resource[nrow(feature_table)] <- moments_of_non_activation_set2$resource[i]
            feature_table$activity[nrow(feature_table)] <- distinct_act_in_queue$activity[j]
            feature_table$outcome[nrow(feature_table)] <- "do_not_activate_batch"
            feature_table$neg_ex_set[nrow(feature_table)] <- 2
            
          }
          
        }
        
        setTxtProgressBar(pb, i)
      }
      close(pb)
    }
    
    # add non-activation snapshots for set 3
    print(paste("Adding feature snapshots for", nrow(set3_creation), "non-activation timestamps (set 3)..."))
    if(nrow(set3_creation) > 0){
      pb <- txtProgressBar(min = 0, max = nrow(set3_creation), style = 3)
      for(i in 1:nrow(set3_creation)){
        # Create feature snapshot and add it to the feature table
        feature_table <- create_feature_snapshot(activity_log = activity_log, feature_table = feature_table,
                                                 act = set3_creation$activity[i], res = set3_creation$resource[i], 
                                                 timestamp = set3_creation$snapshot_time[i], single_queue = set3_creation$single_queue[i],
                                                 timestamp_unit = timestamp_unit, time_unit_result = time_unit_result, aggregate_case_attr = aggregate_case_attr)
        feature_table$feature_timestamp[nrow(feature_table)] <- set3_creation$snapshot_time[i]
        feature_table$resource[nrow(feature_table)] <- set3_creation$resource[i]
        feature_table$activity[nrow(feature_table)] <- set3_creation$activity[i]
        feature_table$outcome[nrow(feature_table)] <- "do_not_activate_batch"
        feature_table$neg_ex_set[nrow(feature_table)] <- 3
        
        setTxtProgressBar(pb, i)
      }
      close(pb)
    }
    
  }
  
  
  # Remove first row of feature table
  feature_table <- feature_table[-1,]
  
  # Convert features time_of_day and hour_of_day to character (otherwise, it will be considered as a numeric attribute)
  feature_table$time_of_day <- as.character(feature_table$time_of_day)
  feature_table$hour_of_day <- as.character(feature_table$hour_of_day)
  
  return(feature_table)
}


# Helper function which returns whether resource is idle at a particular point in time (to generate negative examples set 3)
resource_idle_moment <- function(res, activity_log, moment){
  
  # Determine whether resource activity is recorded at 'moment'
  res_activity <- as.data.frame(activity_log %>% filter(resource == res, start <= moment, complete >= moment))
  
  if(nrow(res_activity) > 0){
    return(FALSE)
  } else{
    return(TRUE)
  }
}

# Helper function which returns resource idle periods within a particular time frame (to generate negative examples set 3)
resource_idle_timeframe <- function(res, activity_log, period_start, period_end){
  
  # Convert timestamp to POSIXct format (if required)
  if(is.character(period_start)){
    period_start <- ymd_hms(period_start)
  }
  if(is.character(period_end)){
    period_end <- ymd_hms(period_end)
  }
  
  # Determine resource activity recorded within [period_start, period_end]
  res_activity <- as.data.frame(activity_log %>% filter(resource == res, 
                                          (start >= period_start && start <= period_end) | (complete >= period_start && complete <= period_end)))
  
  if(nrow(res_activity) > 0){
    # Determine whether idle period is present
    res_activity$idle <- res_activity$resource == res_activity$next_res && res_activity$complete != res_activity$next_start
    
    # Create idle period intervals
    res_activity <- res_activity %>% filter(idle == TRUE)
    res_activity$interval <- as.interval(res_activity$complete,res_activity$next_start)
    
    # Select columns for return dataframe
    res_activity <- res_activity %>% select(start_idle_period = complete, complete_idle_period = next_start, idle_interval = interval)
  } else if(nrow(res_activity) == 0){ # No resource activity is recorded in [period_start, period_end]
    
    # Check if resource is available 
    available <- check_resource_availability(res, activity_log, period_start, period_end)
    
    if(available == TRUE){
      # If the resource is available in the considered period, but no activity is recorded, it is idle during the entire period
      res_activity <- data.frame(start_idle_period = period_start, complete_idle_period = period_end, as.interval(period_start,period_end))
    }
  }
  
  return(res_activity)
}

# Helper function which returns whether a resource is available during a particular time frame
check_resource_availability <- function(res, activity_log, period_start, period_end){
  # Currently implemented as: resource is available when resource activity is recorded on the day that 
  # activity execution started (11/12/2016)
  
  # Determine day start and day end
  day_start <- ymd_hms(paste(as.Date(period_start), " ", "0:00:00", sep = ""))
  day_end <- ymd_hms(paste(as.Date(period_start), " ", "23:59:59", sep = ""))
  
  # Determine whether resource activity is recorded on the day that period start started
  res_activity <- as.data.frame(activity_log %>% filter(resource == res, 
                                                        (start >= day_start && start <= day_end) | (complete >= day_start && complete <= day_end)))
  
  # Return res_available value
  if(nrow(res_activity) == 0){
    return(FALSE)
  } else{
    return(TRUE)
  }
  
}



# PART 3: LEARN DECISION TREES


# Load libraries
library(rpart)
library(C50)
library(RWeka)


# LEARN DECISION TREES USING PACKAGES R-PART OR C50
learn_decision_trees <- function(feature_table, package = "rpart", features_to_exclude = NA){
  
  if(!(package %in% c("rpart", "C50", "RWeka"))){
    stop("This package is not supported. Supported packages are rpart, C50 and RWeka.")
  }
  
  resource_activity_comb <- unique(feature_table[,c('activity', 'resource')])
  resource_activity_comb$id <- paste(resource_activity_comb$resource,'/',resource_activity_comb$activity, sep=" ")
  
  decision_tree_list <- setNames(as.list(rep(NA,nrow(resource_activity_comb))), resource_activity_comb$id)
  
  for(i in 1:nrow(resource_activity_comb)){
    # Create subset of feature table
    feature_table_selection <- feature_table %>% filter(activity == resource_activity_comb$activity[i],
                                                        resource == resource_activity_comb$resource[i]) %>%
                                                select(-c(activity, resource, feature_timestamp))
    if(!is.na(features_to_exclude)[1]){
      for(j in 1:length(features_to_exclude)){
        feature_table_selection[,features_to_exclude[j]] <- NULL
      }
    }
    
    # Remove features that have a single unique value
    if(length(unique(feature_table_selection$day_of_week)) == 1){
      feature_table_selection$day_of_week <- NULL
      warning(paste("Activity", resource_activity_comb$activity[i], "- resource", resource_activity_comb$resource[i], ": feature day_of_week removed for decision tree learning as the feature table only contained a single unique value.", sep = " "))
    }
    if(length(unique(feature_table_selection$part_of_day)) == 1){
      feature_table_selection$part_of_day <- NULL
      warning(paste("Activity", resource_activity_comb$activity[i], "- resource", resource_activity_comb$resource[i], ": feature part_of_day removed for decision tree learning as the feature table only contained a single unique value.", sep = " "))
    }
    if(length(unique(feature_table_selection$hour_of_day)) == 1){
      feature_table_selection$hour_of_day <- NULL
      warning(paste("Activity", resource_activity_comb$activity[i], "- resource", resource_activity_comb$resource[i], ": feature hour_of_day removed for decision tree learning as the feature table only contained a single unique value.", sep = " "))
    }
    
    # Learn (if possible) decision tree
    if (nrow(feature_table_selection) == 0){
      decision_tree_list[[resource_activity_comb$id[i]]] <- "No feature snapshots for this resource-activity combination"
    } else if(length(unique(feature_table_selection$outcome)) == 1){
      decision_tree_list[[resource_activity_comb$id[i]]] <- paste("Only a single outcome is available:", unique(feature_table$outcome)[1])
    } else{
      if(package == "rpart"){
        decision_tree <- rpart(outcome ~ ., data = feature_table_selection)
      } else if(package == "C50"){
        feature_table_selection$outcome <- as.factor(feature_table_selection$outcome)
        decision_tree <- C5.0(outcome ~ ., data = feature_table_selection)
      } else if(package == "RWeka"){
        feature_table_selection <- as.data.frame(unclass(feature_table_selection))
        decision_tree <- J48(outcome ~ ., data = feature_table_selection)
      }
      
      
      # Add decision tree to decision_tree_list
      decision_tree_list[[resource_activity_comb$id[i]]] <- decision_tree
    }
  }
  
  return(decision_tree_list)
}


# LEARN DECISION RULES USING PACKAGE C50
learn_decision_rules <- function(feature_table, package = "C50", features_to_exclude = NA){
  
  if(!(package %in% c("C50"))){
    stop("This package is not supported. Supported packages are C50.")
  }
  
  resource_activity_comb <- unique(feature_table[,c('activity', 'resource')])
  resource_activity_comb$id <- paste(resource_activity_comb$resource,'/',resource_activity_comb$activity, sep="")
  
  decision_rule_list <- setNames(as.list(rep(NA,nrow(resource_activity_comb))), resource_activity_comb$id)
  
  for(i in 1:nrow(resource_activity_comb)){
    # Create subset of feature table
    feature_table_selection <- feature_table %>% filter(activity == resource_activity_comb$activity[i],
                                                        resource == resource_activity_comb$resource[i]) %>%
      select(-c(activity, resource, feature_timestamp))
    if(!is.na(features_to_exclude)[1]){
      for(j in 1:length(features_to_exclude)){
        feature_table_selection[,features_to_exclude[j]] <- NULL
      }
    }
    
    # Remove features that have a single unique value
    if(length(unique(feature_table_selection$day_of_week)) == 1){
      feature_table_selection$day_of_week <- NULL
      warning(paste("Activity", resource_activity_comb$activity[i], "- resource", resource_activity_comb$resource[i], ": feature day_of_week removed for decision tree learning as the feature table only contained a single unique value.", sep = " "))
    }
    if(length(unique(feature_table_selection$part_of_day)) == 1){
      feature_table_selection$part_of_day <- NULL
      warning(paste("Activity", resource_activity_comb$activity[i], "- resource", resource_activity_comb$resource[i], ": feature part_of_day removed for decision tree learning as the feature table only contained a single unique value.", sep = " "))
    }
    if(length(unique(feature_table_selection$hour_of_day)) == 1){
      feature_table_selection$hour_of_day <- NULL
      warning(paste("Activity", resource_activity_comb$activity[i], "- resource", resource_activity_comb$resource[i], ": feature hour_of_day removed for decision tree learning as the feature table only contained a single unique value.", sep = " "))
    }
    
    # Learn (if possible) decision rules
    if(nrow(feature_table_selection) == 0){
      decision_rule_list[[resource_activity_comb$id[i]]] <- "No feature snapshots for this resource-activity combination"
    } else if(length(unique(feature_table_selection$outcome)) == 1){
      decision_rule_list[[resource_activity_comb$id[i]]] <- paste("Only a single outcome is available:", unique(feature_table$outcome)[1])
    } else{
      if(package == "C50"){
        feature_table_selection$outcome <- as.factor(feature_table_selection$outcome)
        decision_rules <- C5.0(outcome ~ ., data = feature_table_selection, rules = TRUE)
      } 
      
      # Add decision tree to decision_tree_list
      decision_rule_list[[resource_activity_comb$id[i]]] <- decision_rules
    }
  }
  
  return(decision_rule_list)
}


# SHOW C5.0 DECISION TREE
show_C50_decision_tree <- function(C50_decision_tree){
  if(class(C50_decision_tree) == "character"){
    return(C50_decision_tree)
  } else{
    return(summary(C50_decision_tree))
  }
}