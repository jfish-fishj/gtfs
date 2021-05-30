#### set up ####
library(data.table)
library(tidyverse)

setwd("/home/jfish/project_data/gtfs/muni/")

#### functions ####
dt.haversine <- function(lat_from, lon_from, lat_to, lon_to, r = 6378137, metric = F){
  if(metric == F){
    r = 3963.190592
  }
  radians <- pi/180
  lat_to <- lat_to * radians
  lat_from <- lat_from * radians
  lon_to <- lon_to * radians
  lon_from <- lon_from * radians
  dLat <- (lat_to - lat_from)
  dLon <- (lon_to - lon_from)
  a <- (sin(dLat/2)^2) + (cos(lat_from) * cos(lat_to)) * (sin(dLon/2)^2)
  return(2 * atan2(sqrt(a), sqrt(1 - a)) * r)
}

replace_hms <- function(x){
  hms = lubridate::hms(x)
  hour = lubridate::hour(hms)
  minute = lubridate::minute(hms)
  second = lubridate::second(hms)
  hour = fifelse(hour >= 24, hour - 24, hour)
  minute = fifelse(minute >= 60, minute - 60, minute)
  second = fifelse(second >= 60, second, second)
  return(lubridate::hms(paste(hour, minute, second, sep = ":")))
}


process_gtfs_folder <- function(folder) {
  #### read in files ####
  # file containing all start-stops in a given time range
  stop_times = fread(
    paste0(folder, "/stop_times.txt"),
    select = c(
      "trip_id",
      "arrival_time",
      "departure_time",
      "stop_id",
      "stop_sequence"
    )
  )
  # file containing all trips 
  trips = fread(paste0(folder, "/trips.txt"))
  # data on each stop (name, lat /long etc)
  stops = fread(
    paste0(folder, "/stops.txt"),
    select = c("stop_id", "stop_name", "stop_lat", "stop_lon")
  )
  # information on when dates happened
  dates = fread(paste0(folder, "/calendar.txt"))
  # route information (names, direction)
  routes = fread(
    paste0(folder, "/routes.txt"),
    select = c(
      "route_id",
      "route_short_name",
      "route_long_name",
      "route_type"
    )
  )
  # dates dont always exist, so only do merge where needed
  if (nrow(dates) > 0) {
    out_file = stop_times %>%
      merge(trips, by = "trip_id") %>%
      merge(stops, by = "stop_id") %>%
      merge(dates, by = "service_id") %>%
      merge(routes, by = "route_id")
  }
  else{
    out_file = stop_times %>%
      merge(trips, by = "trip_id") %>%
      merge(stops, by = "stop_id") %>%
      merge(routes, by = "route_id")
  }
  #### data cleaning ####
  out_file[, folder_date := as.integer(folder)]
  # times are often after 24 hours, which will be NA unless you fix them
  # so 26:02:02 -> 02:02:02
  out_file[order(trip_id, direction_id, stop_sequence)
           , lag_time := shift(departure_time, type="lag"),
           by = .(trip_id, direction_id)]
  out_file[, departure_time := replace_hms(departure_time)]
  out_file[, arrival_time := replace_hms(arrival_time)]
  out_file[, lag_time := replace_hms(lag_time)]
  out_file[, time_between_stop := departure_time - lag_time]
  print(out_file[,list(sum(is.na(departure_time)), sum(is.na(arrival_time)))])
  out_file[, stop_name := str_to_upper(stop_name)]
  # same routes have different capitalizations
  out_file[, route_short_name := str_to_upper(route_short_name)]
  out_file[, route_long_name := str_to_upper(route_long_name)]
  #### data manipulations ####
  # get time and distance between stops, within each trip
 # browser()
  out_file[order(trip_id, direction_id, stop_sequence),
           `:=`(
             previous_stop_name = shift(stop_name, type = "lag"),
             distance_between_stop = dt.haversine(
               lon_to = stop_lon,
               lat_to = stop_lat,
               lon_from = shift(stop_lon, type = "lag"),
               lat_from = shift(stop_lat, type = "lag")
             )
           ), by = .(trip_id, direction_id)]
  out_file[, miles_per_hour := distance_between_stop / (lubridate::time_length(time_between_stop, unit="hour"))]
  return(out_file)
  
}

aggregate_stop_times_hour <- function(out_file) {
  out_file[,hour := as.integer(lubridate::time_length(departure_time, unit="hour"))]
  out_file = out_file[miles_per_hour > 0 &
                        is.finite(miles_per_hour) &
                        hour %in% seq(6, 20)
                      , num_trips := .N, by = .(
                        stop_id,
                        direction_id,
                        #route_id,
                        route_short_name,
                        route_long_name,
                        stop_lat,
                        stop_lon,
                        stop_name,
                        previous_stop_name
                        #stop_sequence
                      )]
  out_agg = out_file[num_trips >= 10,
                     list(
                       mean_miles_per_hour = mean(miles_per_hour, na.rm = T),
                       sd_miles_per_hour = stats::sd(miles_per_hour, na.rm = T),
                       miles_per_hour_decile = quantile(miles_per_hour, probs = seq(0, .9, .1)),
                       distance_between_stop = mean(distance_between_stop, na.rm =
                                                      T),
                       weight = sum(!is.na(miles_per_hour))
                     ), by = .(
                       stop_id,
                       direction_id,
                       #route_id,
                       route_short_name,
                       route_long_name,
                       stop_lat,
                       stop_lon,
                       stop_name,
                       hour,
                       previous_stop_name
                       #stop_sequence
                     )][order(miles_per_hour_decile),
                        decile := paste0("miles_per_hour_decile_", seq(.N)),
                        by = .(
                          stop_id,
                          direction_id,
                          #    route_id,
                          route_short_name,
                          route_long_name,
                          stop_lat,
                          stop_lon,
                          stop_name,
                          previous_stop_name,
                          hour
                          #stop_sequence
                        )]
  out_agg_wide = dcast(
    out_agg,
    formula = ... ~ decile,
    value.var = "miles_per_hour_decile",
    sep = "_"
  )
  
  return(out_agg_wide)
}


#### function calls ####
#temp = process_gtfs_folder("20170410")
stop_times_dt = rbindlist(parallel::mclapply(list.files(pattern="^201[789][0-9]+$"),
                             process_gtfs_folder,
                             mc.silent =F,
                             mc.cores = 4), fill=T)

stop_times_dt_wide_hour = aggregate_stop_times_hour(stop_times_dt)

stop_times_dt_agg_hour = stop_times_dt_wide_hour[,lapply(.SD, weighted.mean, weight=weight, na.rm=T),
                              .SDcols = colnames(stop_times_dt_wide)
                              [str_which(colnames(stop_times_dt_wide), "^miles")],
                              by = .(#stop_id,
                                     direction_id,
                                     #route_id,
                                    route_short_name,
                                     route_long_name,
                                    hour
                                     # stop_lat,
                                     # stop_lon,
                                     #previous_stop_name,
                                     #stop_sequence,
                                     #stop_name
                                     )]

#### check data ####
View(stop_times_dt_agg[route_short_name==29 & 
                         !str_detect(route_short_name, "OWL")][order(direction_id)])
View(stop_times_dt_wide[route_short_name==29 & 
                         !str_detect(route_short_name, "OWL")][order(direction_id)])
View(stop_times_dt_agg_hour[order(-(miles_per_hour_decile_5 / miles_per_hour_decile_2))][ ][1:100])

quantile(stop_times_dt[miles_per_hour > 0 & miles_per_hour < 50 &  
                         str_detect(route_long_name, "EXP|RAP"),
                      miles_per_hour], 
         probs = seq(0,1,.05), na.rm= T)


