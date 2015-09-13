--Author 		: Jojo John Moolayil
--Date			: 07 July 2015
-----------------------------------------------------------------
--Description 
--Creating vectors for Weather Parameters at a Postal code, day level	

--Context
--Averages considers the mean over the past 15 days for the same postal code and weather parameter

--Vectors
--temp_dispersion 			:max_temp - min_temp (for the same day)
--temp_dev_prev_period		:absolute of mean_temp(current_day) - mean temp benchmark for previous period
--temp_dev_period 			:absolute of mean_temp (current day) - mean temp benchmark for current period
--avg_temp_dispersion		:avg of temp_dispersion across past 15 days
--stdev_temp_dispersion		:stdev of temp_dispersion across past 15 days
--stdev_volatility			:stdev of differences of mean with current day and past 15 days
--abs_volatility			:absolute sum of differences of mean with current day and past 15 days
--temp_surge_freq			:freq of cases when current mean temp increasd beyond 3 units from avg benchmarks
--temp_sag_freq				:freq of cases when current mean temp decreased below 3 units from avg benchmarks
-----------------------------------------------------------------

--Creating the skeleton for the final weather data
IF OBJECT_ID('weather_final', 'U') IS NOT NULL
	  DROP TABLE weather_final; 

create table weather_final
	(
		est date,postal_code varchar(100),Max_TemperatureC int,
		Min_TemperatureC int,Mean_TemperatureC int,max_dewpointC int,min_dewpointC int,
		mean_dewpointC int,max_Humidity int,min_humidity int,mean_humidity int,
		Max_sealevelpressurePa int,Min_sealevelpressurePa int,Mean_sealevelpressurePa int,
		Max_VisibilityKm int,Min_VisibilityKm int,Mean_VisibilityKm int,Max_WindSpeed int,
		Mean_WindSpeed int,Max_GustSpeed int,Precipitation float,cloudCover int,Events varchar(100),
		WindDirDegrees varchar(100),Temp_dispersion int,temp_dev_period int,mean_temp15days int,
		stdev_temp_dispersion float,avg_temp_dispersion int,stdev_temp_volatility float,
		abs_temp_volatility int,temp_surge_freq int,temp_sag_freq int,Dew_dispersion int,
		Dew_dev_period int,mean_Dew15days int,stdev_Dew_dispersion float,avg_Dew_dispersion int,
		stdev_dew_volatility float,abs_dew_volatility int,Dew_surge_freq int,Dew_sag_freq int,
		Humidity_dispersion int,Humidity_dev_period int,mean_Humidity15days int,
		stdev_Humidity_dispersion float,avg_Humidity_dispersion int,stdev_humidity_volatility float,
		abs_humidity_volatility int,Humidity_surge_freq int,Humidity_sag_freq int,Pressure_dispersion int,
		Pressure_dev_period int,mean_Pressure15days int,stdev_Pressure_dispersion float,
		avg_Pressure_dispersion int,stdev_Pressure_volatility float,abs_Pressure_volatility int,
		Pressure_surge_freq int,Pressure_sag_freq int,Visibility_dispersion int,Visibility_dev_period int,
		mean_Visibility15days int,stdev_Visibility_dispersion float,avg_Visibility_dispersion int,
		stdev_Visibility_volatility float,abs_Visibility_volatility int,Visibility_surge_freq int,
		Visibility_sag_freq int
	)

--Initiating a cursor to process weather parameters and vectors for each postal code iteratively
--Declare the dynamic variable , i.e. the postal_code which will be iteratively looped for processing
DECLARE @ID varchar(50)

DECLARE weather_data_processing CURSOR FOR
select top 20 postal_code from Weather group by Postal_Code order by  count(distinct est) desc

OPEN weather_data_processing
FETCH NEXT FROM weather_data_processing INTO @ID

WHILE @@FETCH_STATUS = 0
BEGIN


--Preparatory step
--Creating a table with data for exactly 1 postal code
	IF OBJECT_ID('weather_temp', 'U') IS NOT NULL
	  DROP TABLE weather_temp; 

	select 
	est, postal_code,
	cast(max_temperatureC  as int) as Max_TemperatureC,cast(min_temperatureC  as int) as Min_TemperatureC,cast(mean_temperatureC  as int) as Mean_TemperatureC,
	cast(Dew_PointC as int) as max_dewpointC,cast(Min_DewPointC as int) as min_dewpointC,cast(MeanDew_PointC as int) as mean_dewpointC,
	cast(Max_Humidity as int) as  max_Humidity,cast(min_humidity as int) as min_humidity, cast(mean_humidity as int) as mean_humidity,
	cast(Max_Sea_Level_PressurehPa as int) as Max_sealevelpressurePa,cast(Min_Sea_Level_PressurehPa as int) as Min_sealevelpressurePa,cast(Mean_Sea_Level_PressurehPa as int) as Mean_sealevelpressurePa,
	cast(Max_VisibilityKm as int)  as Max_VisibilityKm,cast(Min_VisibilityKm as int)  as Min_VisibilityKm,cast(Mean_VisibilityKm as int)  as Mean_VisibilityKm,
	cast(max_wind_speed as int) as Max_WindSpeed, cast(mean_wind_Speed as int) as Mean_WindSpeed,
	cast(max_gust_speed as int) as Max_GustSpeed,
	cast(case when Precipitationmm= 'T' then '0'  else Precipitationmm end as float) as Precipitation,
	cast(cloudcover as int) as cloudCover,
	Events,
	SUBSTRING(WindDirDegrees,1,charindex('<',WindDirDegrees)-1) as WindDirDegrees
	into weather_temp
	from weather
	where  postal_code = @ID	


--Step 1
--Create a table with self join on postal code and date filters
	IF OBJECT_ID('weather_cross_temp', 'U') IS NOT NULL
	  DROP TABLE weather_cross_temp; 

	select 	a.*,
	--Collecting all weather variables from B Table (Self Join)
	b.est as best,
	b.postal_code as bpostal_code,
	b.Max_TemperatureC as bMax_TemperatureC,b.Min_TemperatureC as bMin_TemperatureC,b.Mean_TemperatureC as bMean_TemperatureC,
	b.max_dewpointC as bmax_dewpointC,b.min_dewpointC as bmin_dewpointC,b.mean_dewpointC as bmean_dewpointC,
	b.max_Humidity as bmax_Humidity,b.min_humidity as bmin_humidity,b.mean_humidity as bmean_humidity,
	b.Max_sealevelpressurePa as bMax_sealevelpressurePa,b.Min_sealevelpressurePa as bMin_sealevelpressurePa,b.Mean_sealevelpressurePa as bMean_sealevelpressurePa,
	b.Max_VisibilityKm as bMax_VisibilityKm,b.Min_VisibilityKm as bMin_VisibilityKm,b.Mean_VisibilityKm as bMean_VisibilityKm,
	b.Max_WindSpeed as bMax_WindSpeed,b.Mean_WindSpeed as bMean_WindSpeed,b.Max_GustSpeed as bMax_GustSpeed,
	b.Precipitation as bPrecipitation,
	b.cloudCover as bcloudCover,
	b.Events as bEvents,
	b.WindDirDegrees as bWindDirDegrees,
	c.yr as  benchyr,c.mnth as  benchmnth,c.part as  benchpart,
	c.max_temp as  benchmax_temp,c.min_temp as  benchmin_temp,c.avg_temp as  benchavg_temp,c.stdev_temp as  benchstdev_temp,
	c.max_dewpoint as  benchmax_dewpoint,c.min_dewpoint as  benchmin_dewpoint,c.avg_dewpoint as  benchavg_dewpoint,c.stdev_dewpoint as  benchstdev_dewpoint,
	c.max_humidity as  benchmax_humidity,c.min_humidity as  benchmin_humidity,c.mean_humidity as  benchmean_humidity,c.stdev_humidity as  benchstdev_humidity,
	c.max_Pressure as  benchmax_Pressure,c.min_Pressure as  benchmin_Pressure,c.mean_Pressure as  benchmean_Pressure,c.stdev_Pressure as  benchstdev_Pressure,
	c.max_Visibility as  benchmax_Visibility,c.min_Visibility as  benchmin_Visibility,c.mean_Visibility as  benchmean_Visibility,c.stdev_Visibility as  benchstdev_Visibility,
	c.max_windspeed as  benchmax_windspeed,c.mean_windspeed as  benchmean_windspeed,c.stdev_windspeed as  benchstdev_windspeed,
	c.max_gust_speed as  benchmax_gust_speed,
	c.mean_rainfall as  benchmean_rainfall,c.stdev_rainfall as  benchstdev_rainfall,
	c.mean_windDirDeg as  benchmean_windDirDeg,
	c.max_cloudcover as  benchmax_cloudcover,c.min_cloudcover as  benchmin_cloudcover,c.avg_cloudcover as  benchavg_cloudcover,c.stdev_cloudcover as  benchstdev_cloudcover,
	c.avg_rain as  benchavg_rain,c.avg_thunder as  benchavg_thunder,c.avg_fog as  benchavg_fog,c.avg_hail as  benchavg_hail,c.avg_snow as  benchavg_snow,c.avg_tornado as  benchavg_tornado
	into weather_cross_temp
	from weather_temp a
	inner join
	weather_temp b
	on a.postal_Code = b.Postal_Code 
	and a.est >= b.est and DATEDIFF(d,b.est,a.est)  between 0 and 15
	inner join
	temp_check2 c
	on a.Postal_Code = c.postal_code and year(a.est) = c.yr and month(a.est) = c.mnth and 
	(case when day(a.est) <= 15 then 'P1' else 'P2' end) = c.part



--Step 	2 : Create vector dimensions for each weather parameter
	
	--Step 2.1

	--Create the Temperature related vectors
	IF OBJECT_ID('weather_temp_int', 'U') IS NOT NULL
	  DROP TABLE weather_temp_int; 

	select postal_code,est, 
	--Creating all Temperature related vectors
	Temp_dispersion,temp_dev_period,
	avg(case when est != best then bMean_TemperatureC end) as mean_temp15days,
	stdev(Temp_dispersionb) as stdev_temp_dispersion, avg(Temp_dispersionb) as avg_temp_dispersion, 
	stdev(abs_mean) as stdev_temp_volatility, sum(abs_mean) as abs_temp_volatility,
	sum(case when bMean_TemperatureC >= (benchavg_temp +3) then 1 else 0 end ) as temp_surge_freq,
	sum(case when bMean_TemperatureC <= (benchavg_temp - 3) then 1 else 0 end ) as temp_sag_freq
	into weather_temp_int -- intermediate table to store data related to Temperature parameters
	from
	(
	select 	a.*,
	--Collecting derived Varaibles at a day Level
	Max_TemperatureC - Min_TemperatureC as Temp_dispersion,
	abs(Mean_TemperatureC  - benchavg_temp) as temp_dev_period,
	(bMax_TemperatureC  - bMin_TemperatureC) as Temp_dispersionb,
	abs(Mean_TemperatureC - bMean_TemperatureC) as abs_mean
	from weather_cross_temp a
	) temp
	group by postal_code,est,Temp_dispersion,temp_dev_period



	--Step 2.2
	--Create the Dewpoint related vectors
	IF OBJECT_ID('weather_dew_int', 'U') IS NOT NULL
	  DROP TABLE weather_dew_int; 

	select postal_code,est, 
	--Creating all Dewpoint related vectors
	Dew_dispersion,Dew_dev_period,
	avg(case when est != best then bMean_DewpointC end) as mean_Dew15days,
	stdev(Dew_dispersionb) as stdev_Dew_dispersion, 
	avg(Dew_dispersionb) as avg_Dew_dispersion, 
	stdev(abs_mean_dewpoint) as stdev_dew_volatility, sum(abs_mean_dewpoint) as abs_dew_volatility,
	sum(case when bMean_DewpointC >= (benchavg_Dewpoint +3) then 1 else 0 end ) as Dew_surge_freq,
	sum(case when bMean_DewpointC <= (benchavg_Dewpoint - 3) then 1 else 0 end ) as Dew_sag_freq
	into weather_Dew_int -- intermediate table to store data related to Dewpoint parameters
	from
	(
	select 	a.*,
	--Collecting derived Varaibles at a day Level
	Max_DewpointC - Min_DewpointC as Dew_dispersion,
	abs(Mean_DewpointC  - benchavg_dewpoint) as Dew_dev_period,
	(bMax_DewpointC  - bMin_DewpointC) as Dew_dispersionb,
	abs(Mean_DewpointC - bMean_DewpointC) as abs_mean_dewpoint
	from weather_cross_temp a
	) Dewpoint
	group by postal_code,est,Dew_dispersion,Dew_dev_period	

	--Step 2.3
	--Creating all Humidity related vectors
	IF OBJECT_ID('weather_humidity_int', 'U') IS NOT NULL
	  DROP TABLE weather_humidity_int; 

	select postal_code,est, 
	Humidity_dispersion,Humidity_dev_period,
	avg(case when est != best then bMean_Humidity end) as mean_Humidity15days,
	stdev(Humidity_dispersionb) as stdev_Humidity_dispersion, 
	avg(Humidity_dispersionb) as avg_Humidity_dispersion, 
	stdev(abs_mean_Humidity) as stdev_humidity_volatility, sum(abs_mean_Humidity) as abs_humidity_volatility,
	sum(case when bMean_Humidity >= (benchmean_Humidity +3) then 1 else 0 end ) as Humidity_surge_freq,
	sum(case when bMean_Humidity <= (benchmean_Humidity - 3) then 1 else 0 end ) as Humidity_sag_freq
	into weather_Humidity_int -- intermediate table to store data related to Humidity parameters
	from
	(
	select 	a.*,
	--Collecting derived Varaibles at a day Level
	Max_Humidity - Min_Humidity as Humidity_dispersion,
	abs(Mean_Humidity  - benchmean_Humidity) as Humidity_dev_period,
	(bMax_Humidity  - bMin_Humidity) as Humidity_dispersionb,
	abs(Mean_Humidity - bMean_Humidity) as abs_mean_Humidity
	from weather_cross_temp a
	) Humidity
	group by postal_code,est,Humidity_dispersion,Humidity_dev_period		


	--Step 2.4
	--Creating all Sea Level Pressure related vectors
	IF OBJECT_ID('weather_pressure_int', 'U') IS NOT NULL
	  DROP TABLE weather_pressure_int; 

	select postal_code,est, 
	Pressure_dispersion,Pressure_dev_period,
	avg(case when est != best then bMean_sealevelpressurePa end) as mean_Pressure15days,
	stdev(Pressure_dispersionb) as stdev_Pressure_dispersion, 
	avg(Pressure_dispersionb) as avg_Pressure_dispersion, 
	stdev(abs_mean_Pressure) as stdev_Pressure_volatility, 
	sum(abs_mean_Pressure) as abs_Pressure_volatility,
	sum(case when bMean_sealevelpressurePa >= (benchmean_Pressure +3) then 1 else 0 end ) as Pressure_surge_freq,
	sum(case when bMean_sealevelpressurePa <= (benchmean_Pressure - 3) then 1 else 0 end ) as Pressure_sag_freq
	into weather_Pressure_int -- intermediate table to store data related to Humidity parameters
	from
	(
	select 	a.*,
	--Collecting derived Varaibles at a day Level
	Max_SeaLevelPressurePa - Min_SeaLevelPressurePa as Pressure_dispersion,
	abs(Mean_SeaLevelPressurePa  - benchmean_Pressure ) as Pressure_dev_period,
	(bMax_SeaLevelPressurePa  - bMin_SeaLevelPressurePa ) as Pressure_dispersionb,
	abs(Mean_SeaLevelPressurePa  - bMean_SeaLevelPressurePa ) as abs_mean_Pressure
	from weather_cross_temp a
	) Pressure
	group by postal_code,est,Pressure_dispersion,Pressure_dev_period		
	


	--Step 2.5
	--Creating all Visiibility related vectors
	IF OBJECT_ID('weather_visibility_int', 'U') IS NOT NULL
	  DROP TABLE weather_visibility_int; 

	select postal_code,est, 
	Visibility_dispersion,Visibility_dev_period,
	avg(case when est != best then bMean_VisibilityKm end) as mean_Visibility15days,
	stdev(Visibility_dispersionb) as stdev_Visibility_dispersion, 
	avg(Visibility_dispersionb) as avg_Visibility_dispersion, 
	stdev(abs_mean_Visibility) as stdev_Visibility_volatility, 
	sum(abs_mean_Visibility) as abs_Visibility_volatility,
	sum(case when bMean_VisibilityKm >= (benchmean_Visibility +3) then 1 else 0 end ) as Visibility_surge_freq,
	sum(case when bMean_VisibilityKm <= (benchmean_Visibility - 3) then 1 else 0 end ) as Visibility_sag_freq
	into weather_Visibility_int -- intermediate table to store data related to Humidity parameters
	from
	(
	select 	a.*,
	--Collecting derived Varaibles at a day Level
	Max_VisibilityKm - min_VisibilityKm as Visibility_dispersion,
	abs(Mean_VisibilityKm  - benchmean_Visibility ) as Visibility_dev_period,
	(bMax_VisibilityKm  - bMin_VisibilityKm ) as Visibility_dispersionb,
	abs(Mean_VisibilityKm  - bMean_VisibilityKm ) as abs_mean_Visibility
	from weather_cross_temp a
	) Pressure
	group by postal_code,est,Visibility_dispersion,Visibility_dev_period		
	
		
--Step 3
-- Intergreate all vectors for the 5 weather parameters into a single table
	insert into weather_final
	select
	a.*,
	--Temperature vectors
	temp.Temp_dispersion,temp.temp_dev_period,temp.mean_temp15days,temp.stdev_temp_dispersion,temp.avg_temp_dispersion,
	temp.stdev_temp_volatility,temp.abs_temp_volatility,temp.temp_surge_freq,temp.temp_sag_freq,
	--Dewpoint vectors
	dew.Dew_dispersion,dew.Dew_dev_period,dew.mean_Dew15days,dew.stdev_Dew_dispersion,dew.avg_Dew_dispersion,
	dew.stdev_dew_volatility,dew.abs_dew_volatility,dew.Dew_surge_freq,dew.Dew_sag_freq,
	--Humidiy vectors
	humidity.Humidity_dispersion,humidity.Humidity_dev_period,humidity.mean_Humidity15days,humidity.stdev_Humidity_dispersion,
	humidity.avg_Humidity_dispersion,humidity.stdev_humidity_volatility,humidity.abs_humidity_volatility,
	humidity.Humidity_surge_freq,humidity.Humidity_sag_freq,
	--Pressure Vectors
	pressure.Pressure_dispersion,pressure.Pressure_dev_period,pressure.mean_Pressure15days,pressure.stdev_Pressure_dispersion,
	pressure.avg_Pressure_dispersion,pressure.stdev_Pressure_volatility,pressure.abs_Pressure_volatility,
	pressure.Pressure_surge_freq,pressure.Pressure_sag_freq,
	--Visibility vectors
	visibility.Visibility_dispersion,visibility.Visibility_dev_period,visibility.mean_Visibility15days,
	visibility.stdev_Visibility_dispersion,visibility.avg_Visibility_dispersion,visibility.stdev_Visibility_volatility,
	visibility.abs_Visibility_volatility,visibility.Visibility_surge_freq,visibility.Visibility_sag_freq
	from
	weather_temp  as a
	inner join 
	weather_temp_int as temp
	on a.postal_code = temp.postal_code and a.est = temp.est
	--Joining the temperature vector table
	inner join
	weather_Dew_int as dew
	on a.postal_code = dew.postal_code and a.est = dew.est
	--Joining Humidity vector table
	inner join 
	weather_Humidity_int as humidity
	on a.postal_code = humidity.postal_code and a.est = humidity.est
	--Joining Pressure vector table
	inner join
	weather_Pressure_int as pressure
	on a.postal_code = pressure.postal_code and a.est = pressure.est
	--Joining Visibility Vector table
	inner join 
	weather_Visibility_int as visibility 
	on a.postal_code = visibility.postal_code and a.est = visibility.est


FETCH NEXT FROM weather_data_processing INTO @ID
END

CLOSE weather_data_processing
DEALLOCATE weather_data_processing
