library(RCurl)
library(tidyverse)

scrape_gtfs <- function(base_url, min_year, max_year, download_dir){
  years = seq(min_year, max_year)
  months = seq(1,12)
  days = seq(1,31)
  ymd = expand_grid(years, months, days)
  url_list = paste0(base_url, ymd)
  for(i in  ymd$temp){
    url = paste0(base_url, i)
    if(RCurl::url.exists(url)){
      download.file(url=url, destfile = paste0(download_dir,i, ".zip" ))
    }
  }
}

scrape_gtfs(
  base_url = "",
  min_year = 2014,
  max_year = 2019,
  download_dir = ""
)


