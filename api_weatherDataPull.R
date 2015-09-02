
#Forecast for the next 15 days : Malpe
weather_forecast<-function(location,days)
{
  location<-"Malpe"
  base_link<-"http://api.worldweatheronline.com/free/v2/weather.ashx"
  key<-"?key=72db368510ed58d56e3800d34a7b4"
  location<-paste0("&q=",location)
  format<-paste0("&format=csv")
  days<-paste0("&num_of_days=",days)
  api_link<-paste0(base_link,key,location,days,format)
  
  #Download data from the API with the preferred settings
  raw_data<-readLines(api_link)
  #Extract and clean data from the file
  weather_header_loc<-10
  astronomy_header_loc<-7
  astronomy_header<-raw_data[astronomy_header_loc]
  weather_header<-raw_data[weather_header_loc]
  raw_data<-raw_data[13:length(raw_data)]
  aseq<-seq(1,length(raw_data),by=9)
  astronomy<-raw_data[aseq]
  astronomy<-gsub('#','',append(astronomy_header,astronomy))
  astronomy <- read.table(textConnection(astronomy), sep = ",", header=TRUE)
  weather<-raw_data[-aseq]
  weather<-gsub('#','',append(weather_header,weather))
  weather <- read.table(textConnection(weather), sep = ",", header=TRUE)
  library(lubridate)
  write.csv(weather,paste0("F:/Delivery/Philips/R outputs/Weather_forecast",Sys.Date(),".csv"))
  write.csv(astronomy,paste0("F:/Delivery/Philips/R outputs/Astronomy_forecast",Sys.Date(),".csv"))

}


  #Historic Data 
 weather_data <- function(location,startdate,enddate)
 {
   base_link<-"http://api.worldweatheronline.com/free/v2/past-weather.ashx"
   key<-"?key=72db368510ed58d56e3800d34a7b4"
   location<-paste0("&q=",location)
   format<-paste0("&format=csv")
   startdate<-paste0("&date=",startdate)
   enddate<-paste0("&enddate=",enddate)
   tp<-"&tp=3"
   api_link<-paste0(base_link,key,location,startdate,enddate,tp,format)
   
    raw_data<-readLines(api_link)
    astronomy_header<-raw_data[3]
    weather_header<-raw_data[6]
    raw_data<-raw_data[9:length(raw_data)]
    aseq<-seq(1,length(raw_data),by=9)
    astronomy<-raw_data[aseq]
    astronomy<-gsub('#','',append(astronomy_header,astronomy))
    astronomy <- read.table(textConnection(astronomy), sep = ",", header=TRUE)
    weather<-raw_data[-aseq]
    weather<-gsub('#','',append(weather_header,weather))
    weather <- read.table(textConnection(weather), sep = ",", header=TRUE)
    
    #Write data to CSV
    write.csv(weather,paste0("F:/Delivery/Philips/R outputs/Weather_Data_",Sys.Date(),".csv"))
    write.csv(astronomy,paste0("F:/Delivery/Philips/R outputs/Astronomy_Data_",Sys.Date(),".csv"))
 }
 
 
 weather_data('Malpe','2015-08-01','2015-08-31')
 weather_forecast('Malpe',5)
 
